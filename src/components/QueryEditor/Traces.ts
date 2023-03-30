import { OpenSearchQuery } from 'types';

export const createDefaultTraceQuery = (query: OpenSearchQuery) => ({
  ...query,
  // TODO this name is bad
  newQueryType: 'traces',
  newQueryObj: {},
});
