import { DataFrameDTO, DataQueryResponse, FieldType, MutableDataFrame } from '@grafana/data';
import { OpenSearchQuery } from 'types';

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
export const createOneTrace = (query: OpenSearchQuery): OpenSearchQuery => ({
  ...query,
  luceneQueryMode: 'traces',
  luceneQueryObj: {
    size: 1000,
    query: {
      bool: {
        must: [
          { range: { startTime: { gte: '$timeFrom', lte: '$timeTo' } } },
          { term: { traceId: '00000000000000000038b5dfd2017015' } }, // Manually Update me! (for now)
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

export const createTraceDataFrame = () => {
  // something here?
};
