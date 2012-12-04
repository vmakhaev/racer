var EventEmitter = require('events').EventEmitter
  , Memory = require('./Memory')
  , eventRegExp = require('./path').eventRegExp
  , uuid = require('node-uuid')
  , Method = require('method')
  , throughStream = require('through')
  , emitStream = require('emit-stream')
  ;

module.exports = Model;

function Model (init) {
  for (var k in init) {
    this[k] = init[k];
  }
  this._memory = new Memory();
  // Set max listeners to unlimited
  this.setMaxListeners(0);

  // Emits events to the outside world
  // It's inverse is this.receiveEmitter
  this.broadcaster = new EventEmitter;
  /**
   * The readable Stream will emit 'data' that encodes the messages we want to
   * pipe to another Stream (e.g., shoe). These messages include:
   * - Local transactions that should be broadcast
   * - Subscription declarations
   * - Unsubscribe declarations
   */
  this.readStream = emitStream(this.broadcaster);
  this.readStream.pause();

  /**
   * This writable Stream allows another Stream to pipe 'data' to the model. This
   * 'data' encodes messages for:
   * - Data to reconcile the model's state with the other Stream's state, for a
   *   given set of subscriptions.
   * - Remote transactions for a given subscription.
   * - Acknowledgments for local transactions.
   *
   * It expects upstream to be an emitStream
   */
  this.writeStream = throughStream(function write (data) {
    this.emit('data', data);
  });
  // An event emitter for incoming remote events
  this.incoming = emitStream(this.writeStream);

  // Used for model scopes
  this._root = this;
  this.mixinEmit('init', this);

  this.middleware = {};
  this.mixinEmit('middleware', this, this.middleware);
}

require('util').inherits(Model, EventEmitter);

Model.prototype.id = function () {
  return uuid.v4();
};

/* Model Duplex Stream */

// TODO Replace 'socket' event mixin listeners
// TODO Include 'fatalErr'
// TODO Include 'unauthorized' event
// TODO Let server ask browser to reload (maybe via 'eval' event?)

Model.prototype.subscribe = Method();

/* Scoped Models */

/**
 * Create a model object scoped to a particular path.
 * Example:
 *     var user = model.at('users.1');
 *     user.set('username', 'brian');
 *     user.on('push', 'todos', function (todo) {
 *       // ...
 *     });
 *
 *  @param {String} segment
 *  @param {Boolean} absolute
 *  @return {Model} a scoped model
 *  @api public
 */
Model.prototype.at = function (segment, absolute) {
  var at = this._at
    , val = (at && !absolute)
          ? (segment === '')
            ? at
            : at + '.' + segment
          : segment.toString()
  return Object.create(this, { _at: { value: val } });
};

/**
 * Returns a model scope that is a number of levels above the current scoped
 * path. Number of levels defaults to 1, so this method called without
 * arguments returns the model scope's parent model scope.
 *
 * @optional @param {Number} levels
 * @return {Model} a scoped model
 */
Model.prototype.parent = function (levels) {
  if (! levels) levels = 1;
  var at = this._at;
  if (!at) return this;
  var segments = at.split('.');
  return this.at(segments.slice(0, segments.length - levels).join('.'), true);
};

/**
 * Returns the path equivalent to the path of the current scoped model plus
 * the suffix path `rest`
 *
 * @optional @param {String} rest
 * @return {String} absolute path
 * @api public
 */
Model.prototype.path = function (rest) {
  var at = this._at;
  if (at) {
    if (rest) return at + '.' + rest;
    return at;
  }
  return rest || '';
};

/**
 * Returns the last property segment of the current model scope path
 *
 * @optional @param {String} path
 * @return {String}
 */
Model.prototype.leaf = function (path) {
  if (!path) path = this._at || '';
  var i = path.lastIndexOf('.');
  return path.substr(i+1);
};

/* Model events */

// EventEmitter.prototype.on, EventEmitter.prototype.addListener, and
// EventEmitter.prototype.once return `this`. The Model equivalents return
// the listener instead, since it is made internally for method subscriptions
// and may need to be passed to removeListener.


Model.prototype._on = Model.prototype.on;

Model.prototype.on = function (type, pattern, callback) {
  var self = this
    , listener = eventListener(type, pattern, callback, this._at);
  this._on(type, listener);
  listener.cleanup = function () {
    self.removeListener(type, listener);
  }
  return listener;
};

Model.prototype.addListener = Model.prototype.on;

Model.prototype.once = function (type, pattern, callback) {
  var listener = eventListener(type, pattern, callback, this._at)
    , self;
  this._on( type, function g () {
    var matches = listener.apply(null, arguments);
    if (matches) this.removeListener(type, g);
  });
  return listener;
};

/**
 * Used to pass an additional argument to local events. This value is added
 * to the event arguments in txns/mixin.Model
 * Example:
 *     model.pass({ ignore: domId }).move('arr', 0, 2);
 *
 * @param {Object} arg
 * @return {Model} an Object that prototypically inherits from the calling
 * Model instance, but with a _pass attribute equivalent to `arg`.
 * @api public
 */
Model.prototype.pass = function (arg) {
  return Object.create(this, { _pass: { value: arg } });
};

/**
 * Returns a function that is assigned as an event listener on method events
 * such as 'set', 'insert', etc.
 *
 * Possible function signatures are:
 *
 * - eventListener(method, pattern, callback, at)
 * - eventListener(method, pattern, callback)
 * - eventListener(method, callback)
 *
 * @param {String} method
 * @param {String} pattern
 * @param {Function} callback
 * @param {String} at
 * @return {Function} function ([path, args...], out, isLocal, pass)
 */
function eventListener (method, pattern, callback, at) {
  if (at) {
    if (typeof pattern === 'string') {
      pattern = at + '.' + pattern;
    } else if (pattern.call) {
      callback = pattern;
      pattern = at;
    } else {
      throw new Error('Unsupported event pattern on scoped model');
    }

    // on(type, listener)
    // Test for function by looking for call, since pattern can be a RegExp,
    // which has typeof pattern === 'function' as well
  } else if ((typeof pattern === 'function') && pattern.call) {
    return pattern;
  }

  // on(method, pattern, callback)
  var regexp = eventRegExp(pattern);

  if (method === 'mutator') {
    return function (mutatorMethod, _arguments) {
      var args = _arguments[0]
        , path = args[0];
      if (! regexp.test(path)) return;

      var captures = regexp.exec(path).slice(1)
        , callbackArgs = captures.concat([mutatorMethod, _arguments]);
      callback.apply(null, callbackArgs);
      return true;
    }
  }

  return function (args, out, isLocal, pass) {
    var path = args[0];
    if (! regexp.test(path)) return;

    args = args.slice(1);
    var captures = regexp.exec(path).slice(1)
      , callbackArgs = captures.concat(args).concat([out, isLocal, pass]);
    callback.apply(null, callbackArgs);
    return true;
  };
}
