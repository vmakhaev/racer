var Method = require('method');

exports = module.exports = {
  subscribe: Method()
, fetch: Method()
, resultPath: Method()
// registerSubscribe
// unregisterSubscribe
// registerFetch
// unregisterFetch
// normalize
// subs
};

var splitPath= require('../path').split;
exports.resultPath.define(String, function (pattern) {
  var pathToGlob = splitPath(pattern)[0];
  return pathToGlob;
});
