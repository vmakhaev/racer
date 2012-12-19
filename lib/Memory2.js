module.exports = Memory;
function Memory() {
  this.flush();
}
Memory.prototype = {
  flush: flush
, get: get
, set: set
, del: del
, push: push
, unshift: unshift
, insert: insert
, pop: pop
, shift: shift
, remove: remove
, move: move
, _pathSegments: pathSegments
, _lookup: lookup
, _lookupSet: lookupSet
, _lookupArray: lookupArray
};

function flush() {
  this.world = {};
  // Used to memoize path splits in lookup
  this.splits = {};
}

function get(collId, docId, prop, path) {
  return this._lookup(collId, docId, prop, path);
}

function set(collId, docId, prop, path, value) {
  // Lookup a pointer to the property or nested property,
  // set the new value, and return the previous value
  function nodeSet(node, key) {
    var previous = node[key];
    node[key] = value;
    return previous;
  }
  return this._lookupSet(collId, docId, prop, path, nodeSet);
}

function del(collId, docId, prop, path) {
  // Lookup a pointer to the property or nested property,
  // delete the property, and return the previous value
  return this._lookupSet(collId, docId, prop, path, nodeDel);
}
function nodeDel(node, key) {
  var previous = node[key];
  delete node[key];
  return previous;
}

function push(collId, docId, prop, path, values) {
  var arr = this._lookupArray(collId, docId, prop, path);
  return arr.push.apply(arr, values);
}

function unshift(collId, docId, prop, path, values) {
  var arr = this._lookupArray(collId, docId, prop, path);
  return arr.unshift.apply(arr, values);
}

function insert(collId, docId, prop, path, index, values) {
  var arr = this._lookupArray(collId, docId, prop, path);
  arr.splice.apply(arr, [index, 0].concat(values));
  return arr.length;
}

function pop(collId, docId, prop, path) {
  var arr = this._lookupArray(collId, docId, prop, path);
  return arr.pop();
}

function shift(collId, docId, prop, path) {
  var arr = this._lookupArray(collId, docId, prop, path);
  return arr.shift();
}

function remove(collId, docId, prop, path, index, howMany) {
  var arr = this._lookupArray(collId, docId, prop, path);
  return arr.splice(index, howMany);
}

function move(collId, docId, prop, path, from, to, howMany) {
  var arr = this._lookupArray(collId, docId, prop, path)
    , len = arr.length
    , values
  // Cast to numbers
  from = +from;
  to = +to;
  // Make sure indices are positive
  if (from < 0) from += len;
  if (to < 0) to += len;
  // Remove from old location
  values = arr.splice(from, howMany);
  // Insert in new location
  arr.splice.apply(arr, [to, 0].concat(values));
  return values;
}

function pathSegments(path) {
  // Split string path at dots and memoize
  return this.splits[path] || (this.splits[path] = path.split('.'));
}

function lookup(collId, docId, prop, path) {
  var coll, doc, node, segments, i, segment

  // Entire world lookup
  if (!collId) return this.world;

  // Collection lookup
  coll = this.world[collId];
  if (!docId) return coll;

  // Document lookup
  if (coll == null) return;
  doc = coll[docId];
  if (!prop) return doc;

  // Property of a document lookup
  if (doc == null) return;
  node = doc[prop];
  if (!path) return node;

  // Nested property lookup
  segments = this._pathSegments(path);
  i = 0;
  while (segment = segments[i++]) {
    if (node == null) return;
    node = node[segment];
  }
  return node;
}

function lookupSet(collId, docId, prop, path, fn) {
  if (!collId || !docId) {
    throw new Error('Set and del require specifying a collection and document');
  }
  var coll, doc, node, segments, i, segment

  // Get or create the collection
  coll = this.world[collId] || (this.world[collId] = {});
  // For setting the entire document
  if (!prop) return fn(coll, docId);

  // Get or create the document
  doc = coll[docId] || (coll[docId] = {});
  // For setting a property of a document
  if (!path) return fn(doc, prop);

  // For setting a nested property
  node = doc;
  segments = this._pathSegments(path);
  i = 0;
  while (segment = segments[i++]) {
    // Get or create implied object or array
    node = node[prop] || (node[prop] = /^\d+$/.test(segment) ? [] : {});
    prop = segment;
  }
  return fn(node, prop);
}

function lookupArray(collId, docId, prop, path) {
  if (!collId || !docId || !prop) {
    throw new Error('Array methods require specifying a collection, document, and property');
  }
  // Lookup a pointer to the property or nested property &
  // return the current value or create a new array
  return this._lookupSet(collId, docId, prop, path, nodeSetArray)
}
function nodeSetArray(node, key) {
  return node[key] || (node[key] = []);
}
