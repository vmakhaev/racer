var Method = require('method');

exports = module.exports = {
  hash: Method()
};

exports.hash.define(String, function (s) {
  return s;
});

var QueryTupleBuilder = require('./descriptor/query/QueryTupleBuilder');
var QueryBuilder = require('./descriptor/query/QueryBuilder');
/**
 * @param {QueryTupleBuilder} qtf
 */
exports.hash.define(QueryTupleBuilder, function (qtf) {
  var tuple = qtf.tuple;
  var registry = qtf.registry;
  var json = registry.queryJSON(tuple);
  return QueryBuilder.hash(json);
});
