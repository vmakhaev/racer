var Method = require('method');
var serializable = require('../protocol.serializable');

exports = module.exports = {
  subscribe: Method()
, fetch: Method()
, resultPath: Method()
, docMatches: Method()
, maybeIncrReadCount: Method()
, decrReadCount: Method()
, fetchMsg: Method()
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

var pathToDoc = require('../path').pathToDoc;

exports.maybeIncrReadCount.define(String, function (s, counts, metadata) {
  var docPath = pathToDoc(s);
  counts[docPath] || (counts[docPath] = 0);
  return ++counts[docPath];
});

exports.decrReadCount.define(String, function (s, counts) {
  var docPath = pathToDoc(s);
  if (! (docPath in counts)) return;
  var left = --counts[docPath];
  if (left === 0) delete counts[docPath];
  var updatedCounts = {};
  updatedCounts[docPath] = left;
  return updatedCounts;
});

var pathUpToStar = require('../path').pathUpToStar;
var isPattern = require('../path').isPattern;

exports.fetchMsg.define(String, function (s, fetches, subscriptions, versions) {
  var msg = { t: serializable.serialize(s) };

  // If s is a path
  if (s.indexOf('*') === -1) {
    var otherFields = [];
    var docPath = pathToDoc(s);
    if (docPath !== s) for (var targetHash in fetches) {
      var currTarget = fetches[targetHash].target;
      if (currTarget === s) continue;
      if (typeof currTarget === 'string') {
        if (! isPattern(currTarget)) {
          var currDocPath = pathToDoc(currTarget);
          if (currDocPath === currTarget) {
            msg.o = currDocPath; // over-ride what we really fetch from the server
            continue;
          }
          if (currDocPath === docPath) {
            var remainder = currTarget.substring(docPath.length + 1, currTarget.length);
            if (remainder) otherFields.push(remainder);
          }
        }
      }
    }
    if (otherFields.length) msg.f = otherFields;
    if (docPath in versions) msg.v = versions[docPath];

  // If s is a pattern
  } else {
    var otherFields = [];
    var prefix = pathUpToStar(s);
    if (prefix.indexOf('.') !== -1) {
      var docPath = pathToDoc(prefix);
      if (docPath in versions) msg.v = versions[docPath];
    } else {
      var relevantVersions = {};
      for (var k in versions) {
        if (k.indexOf(prefix) !== -1) {
          var docId = k.substring(k.indexOf('.') + 1, k.length);
          relevantVersions[docId] = versions[k];
        }
      }
      msg.v = relevantVersions;
    }

    for (var targetHash in fetches) {
      var currTarget = fetches[targetHash].target;
      if (currTarget === s) continue;
      if (typeof currTarget === 'string') {
        if (isPattern(currTarget)) {
          if (currTarget.indexOf('*') === s.indexOf('*')) {
            otherFields.push(currTarget.substring(currTarget.indexOf('*') + 2, currTarget.length));
          }
        }
      }
    }

    if (otherFields.length) msg.f = otherFields;
  }

  return msg;
});
