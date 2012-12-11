var Method = require('method');

module.exports = {
  serialize: Method()
, deserialize: Method()
};

require('./String')(String);
