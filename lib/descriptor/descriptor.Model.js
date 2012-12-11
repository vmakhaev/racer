var Taxonomy = require('./Taxonomy')
  , noop = require('../util').noop
  , normArgs = require('./util').normArgs
  , hashable = require('../protocol.hashable')
  , serializable = require('../protocol.serializable')
  , readable = require('../protocol.readable')
  , World = require('../World')
  ;

module.exports = {
  type: 'Model'

, events: {
    init: function (model) {
      model._fetches = {};
      // TODO Remove universe?
      model.universe = new World;
    }
  }

, decorate: function (Model) {
    // TODO Replace descriptors polymorphism with Method()
    Model.prototype.descriptors = new Taxonomy;
    Model.dataDescriptor = function (conf) {
      var types = Model.prototype.descriptors
        , typeName = conf.name
        , type = types.type(typeName);
      if (type) return type;
      return types.type(typeName, conf);
    };
  }

, proto: {
    /**
     * @param {Array} targets
     * @param {Function} cb(null, scopedModelResults)
     */
    fetch: function (/* targets..., cb*/) {
      var fetchesByHash = this._fetches;
      this.incoming.on('ack.fetch', function (err, data) {
        for (var i = targets.length; i--; ) {
          var hash = targets[i].hash;
          var fetched = fetchesByHash[hash];
          if (fetched.didReceiveSnapshot) {
            // TODO
            continue;
          }
          fetched.didReceiveSnapshot= true;
          var hashes = fetched.docHashes;
          for (var ns in data) {
            var collection = data[ns];
            for (var id in data) {
              hashes[ns + '.' + id] = true;
            }
          }
          universe.addData(data);
        }
      });
      return this.fetch = function (/* targets..., cb */) {
        var numArgs = args.length;
        var targets = Array.prototype.slice.call(arguments, 0);
        var cb = ('function' === typeof targets[numArgs-1])
               ? targets.pop()
               : noop;

        cb.results = [];

        // Allocate space for the results of the fetch
        var fetchesByHash = this._fetches;
        var fetched;
        var universe = this.universe;
        for (var i = targets.length; i--; ) {
          var hash = targets[i].hash = hashable.hash(targets[i]);
          fetched = fetchesByHash[hash];
          if (fetched) {
            if (fetched.didReceiveSnapshot) {
              // TODO Assign to fetched scoped model
              cb.results[i] = fetched;
              // TODO Define maybeCallCb
              if (maybeCallCb) maybeCallCb(cb);
            } else {
              fetched.callbacks.push([cb, i]);
            }
            continue;
          }
          // else not fetched
          fetched = fetchesByHash[hash] = {
            didReceiveSnapshot: false
          , docHashes: {}
          , incomingOpsByDoc: {}
          , callbacks: []
          };
          fetched.callbacks.push([cb, i]);
        }

        var serializedTargets = serializable.serialize(target);
        this.broadcaster.emit('fetch', serializedTargets);
      };

// TODO Adapt and remove
//      descriptors = this.descriptors.normalize(descriptors);
//
//      this.descriptors.handle(this, descriptors, {
//        registerFetch: true
//        // Runs descriptorType.scopedResult and passes return value to this cb
//      , scopedResult: function (scopedModel) {
//          scopedModels.push(scopedModel);
//        }
//      });
//
//      this._upstreamData(descriptors, function (err, data) {
//        if (err) return cb(err);
//        self._addData(data);
//        cb.apply(null, [err].concat(scopedModels));
//      });
    }

  , waitFetch: function (/* descriptors..., cb */) {
      var args = arguments
        , cbIndex = args.length - 1
        , cb = args[cbIndex]
        , self = this

      args[cbIndex] = function (err) {
        if (err === 'disconnected') {
          return self.once('connect', function() {
            self.fetch.apply(self, args);
          });
        };
        cb.apply(null, arguments);
      };
      this.fetch.apply(this, args);
    }

    // TODO Do some sort of subscription counting (like reference counting) to
    // trigger proper cleanup of a query in the QueryRegistry
    /**
     * @param {String|Query} targets[0]
     * @param {String|Query} targets[1]
     * ...
     * @param {Function} cb(err, scopedModels...)
     */
  , subscribe: function (/*targets..., cb*/) {
      var incoming = this.incoming;
      var broadcaster = this.broadcaster;

      var subscriptions = {};
      // subsciptions maps the hash of each target to
      // {
      //   didReceiveSnapshot: Boolean
      // , scopedResult: ScopedModel
      // , target: String|Query
      // }

      this.subscribe = function (/* targets..., cb */) {
        var targets = Array.prototype.slice.call(arguments, 0);
        var cb;
        if (typeof targets[targets.length-1] === 'function') {
          cb = targets.pop();
          // A callback, cb, is associated with the subscription targets with
          // which Model#subscribe(targets..., cb) is called.
          // When we receive subscription data from the server, we will keep
          // track of how many of of these targets have been received and only
          // invoke cb when received count equals expected count.
          cb.received = 0;
          cb.expected = targets.length;
          // The received target data will be turned into ScopedModel instances
          // and appended to cb.args, so we can cb.apply(null, cb.args) in the
          // future.
          cb.args = [null]; // maybe callback with null error
        }

        var serialized = [];
        for (var i = 0, l = targets.length; i < l; i++) {
          var target = targets[i];
          var targetHash = hashable.hash(target);

          // So we can assign the received data to the proper callback
          // argument index later (see `incoming.on('ack.sub', fn)`)
          if (cb) cb[targetHash] = i + 1; // offset by 1 because of err in fn(err, ...)

          var sub = subscriptions[targetHash];
          // If this subscribe includes a redundant subscription target that
          // co-incides with a prior subscribe, then...
          if (sub) {
            // If we've already received the subscription target result, from a
            // prior subscribe...
            if (cb && sub.scopedResult) {
              delete cb[targetHash];
              cb.args[i] = sub.scopedResult;
              if (++cb.received === cb.expected) {
                cb.apply(null, cb.args);
                delete cb.received;
                delete cb.expected;
                delete cb.args;
              }
            }
          } else {
            // If this is the first time in recent memory that we've subscribed
            // to the given target, then keep track of the target.
            sub = subscriptions[targetHash] = {callbacks: [], target: target};
            // And also add it to the subscription message we are going to send
            // over the wire to the server.
            serialized.push(serializable.serialize(target));
          }
          // If we haven't received a result for the given subscription target,
          // then save this callback so we can invoke it when we finally do
          // receive our subscription result.
          if (cb && ! sub.scopedResult) {
            sub.callbacks.push(cb);
          }
        }
        if (! broadcaster.paused) {
          broadcaster.emit('sub', serialized);
        }
      };

      var model = this;
      // Setup generic subscription handler once
      /**
       * @param {Object} data maps docPath -> {snapshot, ops}. It is the
       *                 subscription results, received from the server
       */
      incoming.on('ack.sub', function (data) {
        var docs = data.docs;
        var pointers = data.pointers;

        for (var docPath in docs) {
          var dotIndex = docPath.indexOf('.');
          var ns       = docPath.substring(0, dotIndex);
          var id       = docPath.substring(dotIndex + 1, docPath.length);
          var doc      = docs[docPath];
          var snapshot = doc.snapshot;
          var ops      = doc.ops;

          // Based on data from the server, update our snapshots and versions
          if (snapshot) {
            var ver = snapshot._v_;
            delete snapshot._v_;
            model._memory.set(docPath, snapshot, ver);
          }

          // TODO Handle when we un-subscribed before receiving this data
          for (var targetHash in pointers) {
            var ptr = pointers[targetHash];
            var sub = subscriptions[targetHash];
            var callbacks = sub.callbacks;
            var cb;
            while (cb = callbacks.shift()) {
              var argsIndex = cb[targetHash];
              delete cb[targetHash]; // GC
              var scopedPath = readable.resultPath(sub.target);
              var scopedResult, keyPath;
              if (ptr === true) {
                scopedResult = model.at(targetHash);
              } else if ('id' in ptr) {
                keyPath = scopedPath + '.id';
                model.set(keyPath, ptr.id);
                scopedResult = model.ref(scopedPath, ptr.ns, keyPath);
              } else if ('ids' in ptr) {
                keyPath = scopedPath + '.ids';
                model.set(keyPath, ptr.ids);
                scopedResult = model.refList(scopedPath, ptr.ns, keyPath);
              }
              sub.scopedResult = scopedResult;
              cb.args[argsIndex] = scopedResult;
              if (++cb.received === cb.expected) {
                cb.apply(null, cb.args);
                // GC
                delete cb.received;
                delete cb.expected
                delete cb.args;
              }
            }
          }
        }
      });
      /**
       * @param {String} targetHash
       * @param {String} err
       */
      incoming.on('err.sub', function (targetHash, err) {
        var sub = subscriptions[targetHash];
        var callbacks = sub.callbacks;
        var cb;
        while (cb = callbacks.shift()) {
          cb(err);
          for (var k in cb) delete cb[k]; // GC
        }
      });

      // 'drain' should be emitted when our outgoing IO stream that is reading
      // from this model connects or re-connects.
      this.readStream.on('drain', function () {
        broadcaster.emit('sub', serializedTargets());
      });

      function serializedTargets () {
        var serialized = [];
        for (var targetHash in subscriptions) {
          serialized.push(
            serializable.serialize(subscriptions[targetHash].target)
          );
        }
        return serialized;
      }

      return this.subscribe.apply(this, arguments);
    }

  , subscribeOne: function (target, cb) {
      // And then over-write subscribeOne, so we don't set up our generic
      // subscription handler again.
      this.subscribeOne = function (target, cb) {
        var hash = hashable.hash(target);
        var sub = subscriptions[hash];
      };
      return this.subscribeOne(target, cb);
    }

    // TODO Remove
  , oldSubscribe: function (/* descriptors..., cb */) {
      var args = normArgs(arguments)
        , descriptors = args[0]
        , cb = args[1]
        , self = this

        , scopedModels = []
        ;

      descriptors = this.descriptors.normalize(descriptors);

      // TODO Don't subscribe to a given descriptor again if already
      // subscribed to the descriptor before (so that we avoid an additional fetch)

      this.descriptors.handle(this, descriptors, {
        registerSubscribe: true
      , scopedResult: function (scopedModel) {
          scopedModels.push(scopedModel);
        }
      });

      this._addSub(descriptors, function (err, data) {
        if (err) return cb(err);
        self._addData(data);
        self.emit('addSubData', data);
        cb.apply(null, [err].concat(scopedModels));
      });

      // TODO Cleanup function
      // return {destroy: fn }
    }

  , unsubscribe: function (/* descriptors..., cb */) {
      var args = normArgs(arguments)
        , descriptors = args[0]
        , cb = args[1]
        , self = this
        ;

      descriptors = this.descriptors.normalize(descriptors);

      this.descriptors.handle(this, descriptors, {
        unregisterSubscribe: true
      });

      // if (! descriptors.length) return;

      this._removeSub(descriptors, cb);
    }

  , _upstreamData: function (descriptors, cb) {
      if (!this.connected) return cb('disconnected');
      this.socket.emit('fetch', descriptors, this.scopedContext, cb);
    }

  , _addSub: function (descriptors, cb) {
      if (! this.connected) return cb('disconnected');
      this.socket.emit('subscribe', descriptors, this.scopedContext, cb);
    }

  , _removeSub: function (descriptors, cb) {
      if (! this.connected) return cb('disconnected');
      this.socket.emit('unsubscribe', descriptors, cb);
    }

//    // TODO Associate contexts with path and query subscriptions
//  , _subs: function () {
//      var subs = []
//        , types = this.descriptors
//        , model = this;
//      types.each( function (name, type) {
//        subs = subs.concat(type.subs(model));
//      });
//      return subs;
//    }

  , _addData: function (data) {
      var memory = this._memory;
      data = data.data;

      for (var i = 0, l = data.length; i < l; i++) {
        var triplet = data[i]
          , path  = triplet[0]
          , value = triplet[1]
          , ver   = triplet[2];
        var out = memory.set(path, value, ver);
        // Need this condition for scenarios where we subscribe to a
        // non-existing document. Otherwise, a mutator event would  e emitted
        // with an undefined value, triggering filtering and querying listeners
        // which rely on a document to be defined and possessing an id.
        if (value !== null && typeof value !== 'undefined') {
          // TODO Perhaps make another event to differentiate against model.set
          this.emit('set', [path, value], out);
        }
      }
    }
  }

, server: {
    _upstreamData: function (descriptors, cb) {
      var store = this.store
        , contextName = this.scopedContext
        , self = this;
      this._clientIdPromise.on(function (err, clientId) {
        if (err) return cb(err);
        var req = {
          targets: descriptors
        , clientId: clientId
        , session: self.session
        , context: store.context(contextName)
        };
        var res = {
          fail: cb
        , send: function (data) {
            store.emit('fetch', data, clientId, descriptors);
            cb(null, data);
          }
        };
        store.middleware.fetch(req, res);
      });
    }
  , _addSub: function (descriptors, cb) {
      var store = this.store
        , contextName = this.scopedContext
        , self = this;
      this._clientIdPromise.on(function (err, clientId) {
        if (err) return cb(err);
        // Subscribe while the model still only resides on the server. The
        // model is unsubscribed before sending to the browser.
        var req = {
          clientId: clientId
        , session: self.session
        , targets: descriptors
        , context: store.context(contextName)
        };
        var res = {
          fail: cb
        , send: function (data) {
            cb(null, data);
          }
        };
        store.middleware.subscribe(req, res);
      });
    }
  , _removeSub: function (descriptors, cb) {
      var store = this.store
        , context = this.scopedContext;
      this._clientIdPromise.on(function (err, clientId) {
        if (err) return cb(err);
        var mockSocket = {clientId: clientId};
        store.unsubscribe(mockSocket, descriptors, context, cb);
      });
    }
  }
};
