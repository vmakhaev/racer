var QueryBuilder = require('./QueryBuilder')
  , QueryTupleBuilder = require('./QueryTupleBuilder')
  , bundleUtils = require('../../bundle/util')
  , bundledFunction = bundleUtils.bundledFunction
  , unbundledFunction = bundleUtils.unbundledFunction
  , deepCopy = require('../../util').deepCopy
  ;

module.exports = QueryMotifRegistry;

/**
 * Instantiates a `QueryMotifRegistry`. The instance is used by Model and Store
 * to add query motifs and to generate QueryBuilder instances with the
 * registered query motifs.
 */
function QueryMotifRegistry () {
  // Contains the query motifs declared without a ns.
  // An example this._noNs might look like:
  //     this._noNs = {
  //       motifNameK: callbackK
  //     , motifNameL: callbackL
  //     };
  // This would have been generated via:
  //     this.add('motifNameK', callbackK);
  //     this.add('motifNameL', callbackL);
  this._noNs = {};

  // Contains the query motifs declared with an ns.
  // An example this._byNs might look like:
  //     this._byNs = {
  //       nsA: {
  //         motifNameX: callbackX
  //       , motifNameY: callbackY
  //       }
  //     , nsB: {
  //         motifNameZ: callbackZ
  //       }
  //     };
  // This would have been generated via:
  //     this.add('nsA', 'motifNameX', callbackX);
  //     this.add('nsA', 'motifNameY', callbackY);
  //     this.add('nsB', 'motifNameZ', callbackZ);
  this._byNs = {};

  // An index of builder methods that generate query representations of the form:
  //
  //     { tuple: [ns, {motifName: queryArgs, ...}]}
  //
  // This generated query representation prototypically inherits from
  // this._tupleBuilders[ns] in order to compose queries from > 1 query
  // motifs in a chained manner.
  //
  // An example this._tupleBuilders might look like:
  //
  //     this._tupleBuilders = {
  //       nsA: {
  //         motifNameX: builderX
  //       }
  //     }
  this._tupleBuilders = {};
}

/**
 * Creates a QueryMotifRegistry instance from json that has been generated from
 * QueryMotifRegistry#toJSON
 *
 * @param {Object} json
 * @return {QueryMotifRegistry}
 * @api public
 */
QueryMotifRegistry.fromJSON = function (json) {
  var registry = new QueryMotifRegistry
    , noNs = registry._noNs = json['*'];

  _register(registry, noNs);

  delete json['*'];
  for (var ns in json) {
    var callbacksByName = json[ns];
    _register(registry, callbacksByName, ns);
  }
  return registry;
};

function _register (registry, callbacksByName, ns) {
  for (var motifName in callbacksByName) {
    var callbackStr = callbacksByName[motifName]
      , callback = unbundledFunction(callbackStr);
    if (ns) registry.add(ns, motifName, callback);
    else    registry.add(motifName, callback);
  }
}

/**
 * Registers a query motif.
 *
 * @optional @param {String} ns is the namespace
 * @param {String} motifName is the name of the nquery motif
 * @param {Function} callback
 * @api public
 */
QueryMotifRegistry.prototype.add = function (ns, motifName, callback) {
  if (arguments.length === 2) {
    callback = motifName;
    motifName = ns
    ns = null;
  }
  var callbacksByName;
  if (ns) {
    var byNs = this._byNs;
    callbacksByName = byNs[ns] || (byNs[ns] = Object.create(this._noNs));
  } else {
    callbacksByName = this._noNs;
  }
  if (callbacksByName.hasOwnProperty(motifName)) {
    throw new Error('There is already a query motif "' + motifName + '"');
  }
  callbacksByName[motifName] = callback;

  var tupleBuilders = this._tupleBuilders;
  var builder = tupleBuilders[ns] || (tupleBuilders[ns] = new QueryTupleBuilder);

  builder[motifName] = function addToTuple () {
    var args = Array.prototype.slice.call(arguments);
    // deepCopy the args in case any of the arguments are direct references
    // to an Object or Array stored in our Model Memory. If we don't do this,
    // then we can end up having the query change underneath the registry,
    // which causes problems because the rest of our code expects the
    // registry to point to an immutable query.
    this.tuple[1][motifName] = deepCopy(args);
    return this;
  };
};

/**
 * Unregisters a query motif.
 *
 * @optional @param {String} ns is the namespace
 * @param {String} motifName is the name of the query motif
 * @api public
 */
QueryMotifRegistry.prototype.remove = function (ns, motifName) {
  if (arguments.length === 1) {
    motifName = ns
    ns = null;
  }
  var callbacksByName
    , tupleBuilders = this._tupleBuilders;
  if (ns) {
    var byNs = this._byNs;
    callbacksByName = byNs[ns];
    if (!callbacksByName) return;
    tupleBuilders = tupleBuilders[ns];
  } else {
    callbacksByName = this.noNs;
  }
  if (callbacksByName.hasOwnProperty(motifName)) {
    delete callbacksByName[motifName];
    if (ns && ! Object.keys(callbacksByName).length) {
      delete byNs[ns];
    }
    delete tupleBuilders[motifName];
    if (! Object.keys(tupleBuilders).length) {
      delete this._tupleBuilders[ns];
    }
  }
};

/**
 * Returns an object for composing queries in a chained manner where the
 * chainable methods are named after query motifs registered with a ns.
 *
 * Returned by Model#query(ns)
 *
 * @param {String} ns
 * @return {Object}
 */
QueryMotifRegistry.prototype.queryTupleBuilder = function (ns) {
  var tupleBuilders = this._tupleBuilders[ns];
  if (!tupleBuilders) {
    throw new Error('You have not declared any query motifs for the namespace "' + ns + '"' +
                    '. You must do so via store.query.expose before you can query a namespaced ' +
                    'collection of documents');
  }
  return Object.create(tupleBuilders, {
    tuple: { value: [ns, {}, null] }
  , registry: {value: this}
  });
};

/**
 * Returns a json representation of the query, based on queryTuple and which
 * query motifs happen to be registered at the moment via past calls to
 * QueryMotifRegistry#add.
 *
 * @param {Array} queryTuple is [ns, {motifName: queryArgs}, queryId]
 * @return {Object}
 * @api public
 */
QueryMotifRegistry.prototype.queryJSON = function (queryTuple) {
  // Instantiate a QueryBuilder.
  // Loop through the motifs of the queryTuple, and apply the corresponding motif logic to augment the QueryBuilder.
  // Tack on the query type in the queryTuple (e.g., 'one', 'count', etc.), if
// specified -- otherwise, default to 'find' type.
  // Convert the QueryBuilder instance to json
  var ns = queryTuple[0]
    , queryBuilder = new QueryBuilder({from: ns})

    , queryComponents = queryTuple[1]
    , callbacksByName = this._byNs[ns]
    ;

  for (var motifName in queryComponents) {
    var callback = callbacksByName
                 ? callbacksByName[motifName]
                 : this._noNs[motifName];
    if (! callback) return null;
    var queryArgs = queryComponents[motifName];
    callback.apply(queryBuilder, queryArgs);
  }

  // A typeMethod (e.g., 'one', 'count') declared in query motif chaining
  // should take precedence over any declared inside a motif definition callback
  var typeMethod = queryTuple[2];
  if (typeMethod) queryBuilder[typeMethod]();

  // But if neither the query motif chaining nor the motif definition define
  // a query type, then default to the 'find' query type.
  if (! queryBuilder.type) queryBuilder.find();

  return queryBuilder.toJSON();
};

/**
 * Returns a JSON representation of the registry.
 *
 * @return {Object} JSON representation of `this`
 * @api public
 */
QueryMotifRegistry.prototype.toJSON = function () {
  var json = {}
    , noNs = this._noNs
    , byNs = this._byNs;

  // Copy over query motifs not specific to a namespace
  var curr = json['*'] = {};
  for (var k in noNs) {
    curr[k] = noNs[k].toString();
  }

  // Copy over query motifs specific to a namespace
  for (var ns in byNs) {
    curr = json[ns] = {};
    var callbacks = byNs[ns];
    for (k in callbacks) {
      var cb = callbacks[k];
      curr[k] = bundledFunction(cb);
    }
  }

  return json;
};

/**
 * @param {String} ns is the collection namespace
 * @param {String} motifName is the name of the QueryMotif
 * @return {Number} the arity of the query motif definition callback
 */
QueryMotifRegistry.prototype.arglen = function (ns, motifName) {
  var cbsByMotif = this._byNs[ns];
  if (!cbsByMotif) return;
  var cb = cbsByMotif[motifName];
  return cb && cb.length;
};
