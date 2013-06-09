var Model = require('./index');
var deepEquals = require('deep-is');
var util = require('../util');
var LocalDoc = require('./LocalDoc');
var doDiff = require('./setDiff').doDiff;

Model.INITS.push(function(model) {
  model._virtuals = new VirtualFromMap;
  model.on('all', virtualListener);
  function virtualListener(segments, eventArgs) {
    var collection = segments[0];
    var virtuals = model._virtuals[collection];
    if (! virtuals || ! virtuals.length) return;

    // Listen on inputs and calculate outputs
    for (var i = virtuals.length; i--; ) {
      var virtual = virtuals[i];
      var match = virtual.inputMatch(segments.slice(2));
      if (! match) continue;
      var id = segments[1];
      virtual.emit(model, collection, id, eventArgs);
      var doc = model.getDoc(collection, id);
    }
  }
});

Model.prototype.virtual = function(collectionName, name, opts) {
  var model = this;
  var virtualsMap = this._virtuals;
  var collection = this.getOrCreateCollection(collectionName);
  var Doc = collection.Doc;
  if (! Doc.prototype._virtualGet) {
    Doc.prototype._virtualGet = function (segments) {
      var virtuals = this.model._virtuals[this.collectionName];
      for (var i = virtuals.length; i--; ) {
        var virtual = virtuals[i];
        var match = virtual.outputMatch(segments);
        if (match) {
          var virtualValue = virtual.updateFromInputDoc(this, this.model);
          var remainder = match;
          if (remainder.length) virtualValue = lookup(virtualValue, remainder);
          return virtualValue;
        }
      }
      return;
    };
    var oldGet = Doc.prototype.get;
    Doc.prototype.get = function (segments) {
      var virtualValue = segments && this._virtualGet(segments);
      var out = (virtualValue !== void 0) ?
        virtualValue :
        oldGet.call(this, segments);
      return out;
    };
  }
  var virtuals = virtualsMap[collectionName];
  if (! virtuals) {
    virtuals = virtualsMap[collectionName] = [];
  }
  var virtual = new Virtual(collectionName, name, opts.inputs, opts.get, opts.set);
  virtuals.push(virtual);
};

function lookup (node, segments) {
  for (var i = 0, l = segments.length; i < l; i++) {
    if (! node) return node;
    node = node[segments[i]];
  }
  return node;
}

function VirtualFromMap() {}

function Virtual(collection, output, inputs, get, set) {
  this.collection = collection;
  this.output = output.split('.');
  this.deps = [];
  for (var i = 0, l = inputs.length; i < l; i++) {
    this.deps.push(inputs[i].split('.'));
  }
  this.get = get;
  this.set = set;
  this.cache = {};
}

Virtual.prototype.inputMatch = function(segments) {
  var matches = [];
  var deps = this.deps;
  if (! segments.length) return true;

  for (var i = 0, numDeps = deps.length; i < numDeps; i++) {
    var depSegments = deps[i];
    for (var j = 0, l = segments.length; j < l; j++) {
      var inputPart = segments[j];
      var depPart = depSegments[j];
      if (depPart === '*') {
        // TODO
      } else if (inputPart === depPart) {
        continue;
      } else if ((inputPart === void 0) && (depPart !== void 0)) {
        return depSegments.slice(i);
      } else {
        return false;
      }
    }
    if (depSegments.length === segments.length) return true;
  }
};

Virtual.prototype.outputMatch = function (segments) {
  var outputSegments = this.output;
  if (segments.length < outputSegments.length) return false;
  for (var i = 0, l = outputSegments.length; i < l; i++) {
    var outputSegment = outputSegments[i];
    var testSegment = segments[i];
    if (outputSegment !== '*' && outputSegment !== testSegment) {
      return false;
    }
  }
  var remainder = segments.slice(i);
  return remainder;
};

Virtual.prototype.updateFromInputDoc = function (doc, model) {
  var inputs = [];
  var deps = this.deps;
  for (var i = 0, l = deps.length; i < l; i++) {
    var segments = deps[i];
    inputs.push(doc.get(segments));
  }
  var hash = doc.collectionName + '.' + doc.id;
  var virtualDoc = this.cache[hash] ||
    (this.cache[hash] = new LocalDoc(doc.collectionName, doc.id));
  var outputSegments = this.output;
  var before = virtualDoc.get(outputSegments);
  var after = this._fromInput(inputs);
  var group = util.asyncGroup(function (err) {if (err) console.error(err);});
  doDiff(model, virtualDoc, [doc.collectionName, doc.id].concat(outputSegments), before, after, deepEquals, group);
  return after;
};

Virtual.prototype._fromInput = function(inputs) {
  return this.get.apply(null, inputs);
};

Virtual.prototype.emit = function(model, collection, id, eventArgs) {
  var doc = model.getDoc(collection, id);
  this.updateFromInputDoc(doc, model);
};
