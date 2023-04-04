import { DataFrame, DataFrameDTO, DataQueryResponse, FieldType, MutableDataFrame, TraceSpanRow } from '@grafana/data';
import { OpenSearchQuery, OpenSearchSpan, OpenSearchSpanEvent, QueryType } from 'types';
import { createEmptyDataFrame } from './utils';
import { set } from 'lodash';
import { TraceKeyValuePair, TraceLog } from '@grafana/data/types/trace';

export const createDefaultTraceQuery = (query: OpenSearchQuery): OpenSearchQuery => ({
  ...query,
  luceneQueryMode: 'traces',
  luceneQueryObj: {
    size: 10,
    query: {
      bool: {
        must: [{ range: { startTime: { gte: '$timeFrom', lte: '$timeTo' } } }],
        filter: [],
        should: [],
        must_not: [],
      },
    },
    aggs: {
      // create a set of buckets that we call traces
      traces: {
        // each of those buckets in traces is sorted by a key of their traceId
        // they contain any document, in this case all the spans of a trace
        terms: {
          field: 'traceId',
          size: 100,
          order: { _key: 'asc' },
        },
        // within each of those buckets we create further aggregations based on what's in that bucket
        aggs: {
          // one of those aggregations is a metric we call latency which is based on the durationInNanos
          // this script was taken directly from the network tab in the traces dashboarhd
          latency: {
            max: {
              script: {
                source:
                  "\n                if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) {\n                  return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0\n                }\n                return 0\n                ",
                lang: 'painless',
              },
            },
          },
          // one of those aggregations is the first traceGroup value it finds in the bucket
          trace_group: {
            terms: {
              field: 'traceGroup',
              size: 1,
            },
          },
          // one of aggregations is the the number of items in the bucket that has a status code of 2
          error_count: {
            filter: { term: { 'traceGroupFields.statusCode': '2' } },
          },
          // one of those aggregations is the span with the max endTime
          last_updated: { max: { field: 'traceGroupFields.endTime' } },
        },
      },
    },
  },
});

/* 
{"size":1000,"query":{"bool":{"must":[{"term":{"traceId":"00000000000000001c10de244eb9421a"}}],"filter":[],"should":[],"must_not":[]}},"index":"otel-v1-apm-span-*"}
*/
// temporary function to request one particular trace while we work on visualizing traces
export const createSingleTraceQuery = (query: OpenSearchQuery): OpenSearchQuery => ({
  ...query,
  isSingleTrace: true,
  luceneQueryMode: 'traces',
  luceneQueryObj: {
    size: 1000,
    query: {
      bool: {
        must: [
          { range: { startTime: { gte: '$timeFrom', lte: '$timeTo' } } },
          { term: { traceId: '0000000000000000061141afde96cda8' } }, // Manually Update me! (for now)
        ],
        filter: [],
        should: [],
        must_not: [],
      },
    },
  },
});

export const createTracesDataFrame = (targets, response): DataQueryResponse => {
  const traceIds = [];
  const traceGroups = [];
  const latency = [];
  const errors = [];
  const lastUpdated = [];

  response[0].aggregations.traces.buckets.forEach(bucket => {
    traceIds.push(bucket.key);
    traceGroups.push(bucket.trace_group.buckets[0].key);
    latency.push(bucket.latency.value);
    errors.push(bucket.error_count.doc_count);
    lastUpdated.push(bucket.last_updated.value);
  });

  const traceFields: DataFrameDTO = {
    fields: [
      { name: 'Trace Id', type: FieldType.string, values: traceIds },
      { name: 'Trace Group', type: FieldType.string, values: traceGroups },
      { name: 'Latency (ms)', type: FieldType.number, values: latency },
      // { name: 'Percentile in trace group', type: FieldType.string, values: ['todo'] },
      { name: 'Error Count', type: FieldType.number, values: errors },
      { name: 'Last Updated', type: FieldType.time, values: lastUpdated },
    ],
  };
  const dataFrames = new MutableDataFrame(traceFields);
  return { data: [dataFrames], key: targets[0].refId };
};

export const createTraceDataFrame = (targets, response): DataQueryResponse => {
  const spanFields = [
    'traceID',
    'durationInNanos',
    'serviceName',
    'parentSpanID',
    'spanID',
    'operationName',
    'startTime',
    'duration',
    'tags',
    'serviceTags',
    'stackTraces',
    'logs',
  ];

  let series = createEmptyDataFrame(spanFields, '', false, QueryType.Lucene);
  const dataFrames: DataFrame[] = [];
  const spans = transformTraceResponse(response[0].hits.hits);
  // Add a row for each document
  for (const doc of spans) {
    series.add(doc);
  }
  // do we need this?
  series.refId = targets[0].refId;
  dataFrames.push(series);

  return { data: dataFrames, key: targets[0].refId };
};

function transformTraceResponse(spanList: OpenSearchSpan[]): TraceSpanRow[] {
  return spanList.map(span => {
    const spanData = span._source;
    // some k:v pairs come from OpenSearch in dot notation: 'span.attributes.http@status_code': 200,
    // namely TraceSpanRow.Attributes and TraceSpanRow.Resource
    // this turns everything into objects we can group and display
    const nestedSpan = {} as OpenSearchSpan;
    Object.keys(spanData).map(key => {
      set(nestedSpan, key, spanData[key]);
    });
    const hasError = nestedSpan.events ? spanHasError(nestedSpan.events) : false;

    return {
      ...nestedSpan,
      parentSpanID: nestedSpan.parentSpanId,
      traceID: nestedSpan.traceId,
      spanID: nestedSpan.spanId,
      operationName: nestedSpan.name,
      // grafana needs time in milliseconds
      startTime: new Date(nestedSpan.startTime).getTime(),
      duration: nestedSpan.durationInNanos * 0.000001,
      tags: [
        ...convertToKeyValue(nestedSpan.span?.attributes ?? {}),
        // TraceView needs a true or false value here to display the error icon next to the span
        { key: 'error', value: hasError },
      ],
      serviceTags: nestedSpan.resource?.attributes ? convertToKeyValue(nestedSpan.resource.attributes) : [],
      ...(hasError ? { stackTraces: getStackTraces(nestedSpan.events) } : {}),
      logs: nestedSpan.events.length ? transformEventsIntoLogs(nestedSpan.events) : [],
    };
  });
}

function spanHasError(spanEvents: OpenSearchSpanEvent[]): boolean {
  return spanEvents.some(event => event.attributes.error);
}

function getStackTraces(events: OpenSearchSpanEvent[]) {
  const stackTraces = events
    .filter(event => event.attributes.error)
    .map(event => `${event.name}: ${event.attributes.error}`);
  // if we return an empty array, Trace panel plugin shows "0"
  return stackTraces.length > 0 ? stackTraces : undefined;
}

function convertToKeyValue(tags: Record<string, any>): TraceKeyValuePair[] {
  return Object.keys(tags).map(key => ({
    key,
    value: tags[key],
  }));
}

function transformEventsIntoLogs(events: OpenSearchSpanEvent[]): TraceLog[] {
  return events.map(event => ({
    timestamp: new Date(event.time).getTime(),
    fields: [{ key: 'name', value: event.name }],
  }));
}
