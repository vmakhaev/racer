var Method = require('method');

module.exports = {
  serialize: Method()
, deserialize: Method()
};

require('./String')(String);

var QueryTupleBuilder = require('../descriptor/query/QueryTupleBuilder');
require('./QueryTupleBuilder')(QueryTupleBuilder);
