var Method = require('method');

exports = module.exports = {
  hash: Method()
};

exports.hash.define(String, function (s) {
  return s;
});
