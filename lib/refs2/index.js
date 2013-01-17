exports = module.exports = plugin;
exports.useWith = { server: true, browser: true };
exports.decorate = 'racer';

function plugin (racer) {
  racer.mixin(mixin);
}

var mixin = {
  type: 'Model'
// , server: __dirname + '/refs.server'
, events: {
    init: init
  , bundle: bundle
  }
, proto: {
    ref2: ref
  , removeRef2: removeRef
  , dereference2: dereference
  }
};

function ref(from, to) {
  // TODO: Model aliases
  this._refMap[from] = to;
  return this.at(to);
}

function dereference(path) {
  var current = path
    , i = 999
    , match, to
  while (i--) {
    // Find the first three path segments, such as "coll.doc.prop"
    // and the remaining path after that
    match = /^([^.]+\.[^.]+\.[^.]+)($|\..*)/.exec(current);
    to = match && this._refMap[match[1]];
    if (!to) return current;
    current = to + match[2];
  }
  throw new Error('Maximum dereferences exceeded with path: ' + path);
}

function removeRef(from) {
  delete this._refMap[from];
}

function init(model) {
  model._refMap = {};

  // De-reference transactions to operate on their absolute path
  model.on('beforeTxn', function (method, args) {
    args[0] = model.dereference2(args[0]);
  });
}

function bundle(model) {
  var onLoad = model._onLoad
}