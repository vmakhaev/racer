var protocol = require('./index');

module.exports = function (QueryTupleBuilder) {
  protocol.serialize.define(QueryTupleBuilder, function (query) {
    return query.tuple;
  });
};
