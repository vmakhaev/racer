var Method = require('method');
var serializable = require('../protocol.serializable');

exports = module.exports = {
//  subscribe: Method()
//, fetch: Method()
  resultPath: Method()
, docMatches: Method()
, maybeIncrReadCount: Method()
, decrReadCount: Method()
, readMsg: Method()
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
  var updatedCounts = {};
  var docPath = pathToDoc(s);
  if (! (docPath in counts)) return updatedCounts;
  var left = --counts[docPath];
  if (left === 0) delete counts[docPath];
  updatedCounts[docPath] = left;
  return updatedCounts;
});

var pathUpToStar = require('../path').pathUpToStar;
var isPattern = require('../path').isPattern;

exports.readMsg.define(String, function (s, fetches, subscriptions, versions) {
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


var QueryTupleBuilder = require('../descriptor/query/QueryTupleBuilder');
var QueryBuilder = require('../descriptor/query/QueryBuilder');

exports.resultPath.define(QueryTupleBuilder, function (query, model) {
  var tuple = query.tuple;
  var queryMotifRegistry = model._queryMotifRegistry;

  // Out tuple only encodes queries at the language level of query motifs. In
  // order to encode queries at the language level of core query descriptors,
  // we need to pass our tuple through our QueryMotifRegistry.
  var queryJson = queryMotifRegistry.queryJSON(tuple);

  // With queryJson, we can generate a query id that is going to be interpreted
  // the same way on the server and on the client.
  var queryId = QueryBuilder.hash(queryJson);
  var prefix = '_$queries.' + queryId;

  var type = queryJson.type;
  if (type === 'count') {
    return prefix + '.count';
  }

  if (type === 'find') {
    return prefix + '.' + queryId + '.results';
  }

  if (type === 'findOne' || type === 'one') {
    return prefix + '.' + queryId + 'result';
  }
});

exports.readMsg.define(QueryTupleBuilder, function (query, fetches, subscriptions, versions) {
  return {t: serializable.serialize(query) };
});

exports.maybeIncrReadCount.define(QueryTupleBuilder, function (query, counts, metadata) {
  var scopedResult = metadata.scopedResult;
  if (! scopedResult) return;

  var result = scopedResult.get();
  if (! result) return;

  var ns = query.tuple[0];
  if (Array.isArray(result)) {
    for (var i = result.length; i--; ) {
      var docPath = ns + '.' + result[i].id;
      counts[docPath] || (counts[docPath] = 0);
      ++counts[docPath];
    }
  } else {
    var docPath = ns + '.' + result.id;
    counts[docPath] || (counts[docPath] = 0);
    ++counts[docPath];
  }
});

exports.decrReadCount.define(QueryTupleBuilder, function (query, counts, metadata) {
  var updatedCounts = {};
  var scopedResult = metadata.scopedResult;
  if (! scopedResult) return updatedCounts;

  var result = scopedResult.get();
  if (! result) return updatedCounts;

  var ns = query.tuple[0];
  if (Array.isArray(result)) {
    for (var i = result.length; i-- ; ) {
      var docPath = ns + '.' + result[i].id;
      if (! (docPath in counts)) continue;
      var left = --counts[docPath];
      if (left === 0) delete counts[docPath];
      updatedCounts[docPath] = left;
    }
  } else {
    var docPath = ns + '.' + result.id;
    if (! (docPath in counts)) return updatedCounts;
    var left = --counts[docPath];
    if (left === 0) delete counts[docPath];
    updatedCounts[docPath] = left;
  }
  return updatedCounts;
});
