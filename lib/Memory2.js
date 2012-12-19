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
, _lookupSet: lookupSet
, _lookupArray: lookupArray
};

function flush() {
  this.world = {};
}

function set(segments, value) {
  if (segments.length < 2) {
    throw new Error('Set requires specifying a collection and document');
  }
  // Lookup a pointer to the property or nested property,
  // set the new value, and return the previous value
  function nodeSet(node, key) {
    var previous = node[key];
    node[key] = value;
    return previous;
  }
  return this._lookupSet(segments, nodeSet);
}

function del(segments) {
  if (segments.length < 2) {
    throw new Error('Del requires specifying a collection and document');
  }
  // Don't do anything if the value is already undefined, since
  // lookupSet creates objects as it traverses, and the del
  // method should not create anything
  var previous = this.get(segments);
  if (previous === void 0) return;
  // Lookup a pointer to the property or nested property,
  // delete the property, and return the previous value
  this._lookupSet(segments, nodeDel);
  return previous;
}
function nodeDel(node, key) {
  delete node[key];
}

function push(segments, values) {
  var arr = this._lookupArray(segments);
  return arr.push.apply(arr, values);
}

function unshift(segments, values) {
  var arr = this._lookupArray(segments);
  return arr.unshift.apply(arr, values);
}

function insert(segments, index, values) {
  var arr = this._lookupArray(segments);
  arr.splice.apply(arr, [index, 0].concat(values));
  return arr.length;
}

function pop(segments) {
  var arr = this._lookupArray(segments);
  return arr.pop();
}

function shift(segments) {
  var arr = this._lookupArray(segments);
  return arr.shift();
}

function remove(segments, index, howMany) {
  var arr = this._lookupArray(segments);
  return arr.splice(index, howMany);
}

function move(segments, from, to, howMany) {
  var arr = this._lookupArray(segments)
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

function get(segments) {
  if (!segments) return this.world;
  var node = this.world
    , i = 0
    , segment
  while (segment = segments[i++]) {
    if (node == null) return;
    node = node[segment];
  }
  return node;
}

function lookupSet(segments, fn) {
  var coll, doc, node, key, i, segment

  // Get or create the collection
  coll = this.world[segments[0]] || (this.world[segments[0]] = {});
  // For setting the entire document
  if (!segments[2]) return fn(coll, segments[1]);

  // Get or create the document
  doc = coll[segments[1]] || (coll[segments[1]] = {});
  // For setting a property of a document
  if (!segments[3]) return fn(doc, segments[2]);

  // For setting a nested property
  node = doc;
  key = segments[2];
  i = 3;
  while (segment = segments[i++]) {
    // Get or create implied object or array
    node = node[key] || (node[key] = /^\d+$/.test(segment) ? [] : {});
    key = segment;
  }
  return fn(node, key);
}

function lookupArray(segments) {
  if (segments.length < 3) {
    throw new Error('Array methods require specifying a collection, document, and property');
  }
  // Lookup a pointer to the property or nested property &
  // return the current value or create a new array
  var arr = this._lookupSet(segments, nodeSetArray)

  if (!Array.isArray(arr)) {
    throw new TypeError('Array method called on non-array at ' +
      segments.join('.') + ': ' + JSON.stringify(arr, null, 2)
    );
  }
  return arr;
}
function nodeSetArray(node, key) {
  return node[key] || (node[key] = []);
}
