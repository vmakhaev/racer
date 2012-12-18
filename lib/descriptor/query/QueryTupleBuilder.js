var queryTypes = require('./types');

module.exports = QueryTupleBuilder;

function QueryTupleBuilder () {
}
var tupleFactoryProto = QueryTupleBuilder.prototype;
for (var type in queryTypes) {
  (function (type) {
    // t could be: 'find', 'one', 'count', etc. -- see ./types
    tupleFactoryProto[type] = function () {
      this.tuple[2] = type;
      return this;
    };
  })(type);
}
