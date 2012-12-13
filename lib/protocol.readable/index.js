var Method = require('method');

exports = module.exports = {
  subscribe: Method()
, fetch: Method()
, resultPath: Method()
, docMatches: Method()
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

/**
 * Given a fetch or subscribe string, this returns an object that
 * represents the versions of the local documents that happen to match this
 * particular string.
 *
 * @param {String} s
 * @param {Model} model
 * @return {Object} {ns: String, v: {<id>: version, <id>: version, ...}}
 */
exports.docMatches.define(String, function (s, model) {
  var parts = s.split('.');
  var versions = {};
  if (s.indexOf('*') === -1) {
    var docPath = parts.slice(0, 2).join('.');
    versions[parts[1]] = model.version(docPath);
    return {ns: parts[0], v: versions};
  }

  if (parts[0] === '*') {
    throw new Error('Unimplemented');
  }

  // a.*
  if (parts[1] === '*') {
    // a.*.b
    if (parts.length > 2) {
      throw new Error('Unimplemented');
    // a.*
    } else {
      throw new Error('Unimplemented');
    }
  }
  throw new Error('Unimplemented');
});
