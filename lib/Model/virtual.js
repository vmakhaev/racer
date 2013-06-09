var Model = require('./index');

Model.INITS.push(function(model) {
  model._virtuals = new VirtualFromMap;
//  model.on('all', virtualListener);
  function virtualListener(segments, eventArgs) {
    var collection = segments[0];
    var virtuals = model._virtuals[collection];
    if (! virtuals || ! virtuals.length) return;
    for (var i = virtuals.length; i--; ) {
      var virtual = virtuals[i];
      var matches = virtual.inputMatch(segments);
      var output = virtual.get(matches);
      var id = segments[1];
      var doc = model.getDoc(collection, id);
    }
  }
});

Model.prototype.virtual = function(collectionName, name, opts) {
  var virtualsMap = this._virtuals;
  var collection = this.getOrCreateCollection(collectionName);
  var Doc = collection.Doc;
  if (! Doc.prototype._virtualGet) {
    Doc.prototype._virtualGet = function (segments) {
      var virtuals = virtualsMap[this.collectionName];
      for (var i = virtuals.length; i--; ) {
        var virtual = virtuals[i];
        if (virtual.outputMatch(segments)) {
          return virtual.fromInputDoc(this);
        }
      }
      return;
    };
    var oldGet = Doc.prototype.get;
    Doc.prototype.get = function (segments) {
      var virtualValue = this._virtualGet(segments);
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
}

Virtual.prototype.inputMatch = function(segments) {
  var deps = this.deps;
  for (var i = 0, l = segments.length; i < l; i++) {
    var inputPart = segments[i];
    for (var j = 0, numDeps = deps.length; j < numDeps; j++) {
      var depSegments = deps[j];
      var depPart = depSegments[i];
      if (depPart === '*') {
      } else if (inputPart === depPart) {
      } else {
      }
    }
  }
};

Virtual.prototype.outputMatch = function (segments) {
  var outputSegments = this.output;
  for (var i = 0, l = segments.length; i < l; i++) {
    var testSegment = segments[i];
    var outputSegment = outputSegments[i];
    if (outputSegment !== '*' && outputSegment !== testSegment) {
      return false;
    }
  }
  return true;
};

Virtual.prototype.fromInputDoc = function (doc) {
  var inputs = [];
  var deps = this.deps;
  for (var i = 0, l = deps.length; i < l; i++) {
    var segments = deps[i];
    inputs.push(doc.get(segments));
  }
  return this.fromInput(inputs);
};

Virtual.prototype.fromInput = function(inputs) {
  return this.get.apply(null, inputs);
};
