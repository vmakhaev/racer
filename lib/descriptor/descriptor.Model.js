var Taxonomy = require('./Taxonomy')
  , normArgs = require('./util').normArgs
  , hashable = require('../protocol.hashable')
  , serializable = require('../protocol.serializable')
  , readable = require('../protocol.readable')
  , World = require('../World')
  , transaction = require('../transaction')
  , pathUtils    = require('../path')
  , pathToDoc    = pathUtils.pathToDoc
  , isPrivate    = pathUtils.isPrivate
  , pathUpToStar = pathUtils.pathUpToStar
  ;

module.exports = {
  type: 'Model'

, events: {
    init: function (model) {
      // These both map target hashes to metadata about the target and
      // about its results, i.e., maps the hash of each target to:
      // {
      //   didReceiveSnapshot: Boolean
      // , scopedResult: ScopedModel
      // , target: String|Query
      // }
      var subscriptions = model._subscriptions = {};
      var fetches = model._fetches = {};

      // Keeps track of which documents are still associated with a fetch or subscribe
      model._docReadCounts = {};

      var incoming = model.incoming;
      incoming.on('ack.sub', function (data) {
        model._onReadAck(data, subscriptions);
      });
      incoming.on('err.sub', function (targetHash, err) {
        model._onReadErr(targetHash, err, subscriptions);
      });

      incoming.on('ack.fetch', function (data) {
        model._onReadAck(data, fetches);
      });
      incoming.on('err.fetch', function (targetHash, err) {
        model._onReadErr(targetHash, err, fetches);
      });

      var broadcaster = model.broadcaster;

      // 'drain' is emitted when our outgoing IO stream that is reading
      // from this model connects or re-connects.
      model.readStream.on('drain', function () {
        var payload = serializedTargets(subscriptions);
        var subId = model.id();
        payload.unshift(subId);
        if (payload.length > 1) {
          broadcaster.emit('sub', payload);
        }
        payload = serializedTargets(fetches);
        var fetchId = model.id()
        payload.unshift(fetchId);
        if (payload.length > 1) {
          broadcaster.emit('fetch', payload);
        }
      });

      broadcaster.shouldResend('sub', {
        every: 400
      , until: ['ack.sub'] // TODO Or on Model#unsubscribe
//      , until: function (cleanup) { ... }
      , broadcastHash: function (payload) {
          var subId = payload[0];
          return subId;
        }
      , untilHash: function (data) {
          var subId = data.id;
          return subId;
        }
      });

      broadcaster.shouldResend('unsub', {
        every: 400
      , until: ['ack.unsub']
      , broadcastHash: function (hashed) {
          var unSubId = hashed[0];
          return unSubId;
        }
      , untilHash: function (unsubId) {
          return unSubId;
        }
      });

      broadcaster.shouldResend('fetch', {
        every: 400
      , until: ['ack.fetch']
      , broadcastHash: function (payload) {
          var fetchId = payload[0];
          return fetchId;
        }
      , untilHash: function (data) {
          var fetchId = data.id;
          return fetchId;
        }
      });
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
     * @param {String|Query} targets[0]
     * @param {String|Query} targets[1]
     * ...
     * @param {Function} cb(null, scopedModelResults)
     */
    fetch: function (/* targets..., cb */) {
      var targets = Array.prototype.slice.call(arguments, 0);
      var cb;
      if (typeof targets[targets.length-1] === 'function') {
        cb = targets.pop();
      }

      var sendIfAlreadyLocal = true;
      var payload = prepReadTargets(targets, this._fetches, cb, sendIfAlreadyLocal, this);

      var broadcaster = this.broadcaster;
      if ((! broadcaster.paused) && payload.length > 1) {
        broadcaster.emit('fetch', payload);
      }
      return {id: payload[0]};

      // TODO The following is considered in master, but potentially not here:
      // If there are still transactiosn we are waiting for server
      // acknowledgment (i.e., 'txnOk') from, sometimes we may run into
      // scenarios where the upstream data we receive already reflects
      // mutations applied by these not yet ack'ed transactions. In this
      // case, we want to wait for ack of these transactions before adding
      // data and calling back to our app. If we do not, then the memory's
      // world version would be set to the upstream data's max version;
      // this would be bad because we would then ignore the transaction
      // acks we are waiting on because they would be less than the version
      // of the upstream data; the result is we would re-send the same
      // transactions to the server, resulting in double-mutations or more
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

  , alwaysSubscribe: function (/* descriptors..., cb */) {
      // TODO See master
      throw new Error('Unimplemented');
    }

    // TODO Do some sort of subscription counting (like reference counting) to
    // trigger proper cleanup of a query in the QueryRegistry
    //
    // TODO Things that are subscribable should implement:
    //      - readable.readMsg
    //      - serializable.serialize
    /**
     * @param {String|Query} targets[0]
     * @param {String|Query} targets[1]
     * ...
     * @param {Function} cb(err, scopedModels...)
     */
  , subscribe: function (/* targets..., cb */) {
      var targets = Array.prototype.slice.call(arguments, 0);
      var cb;
      if (typeof targets[targets.length-1] === 'function') {
        cb = targets.pop();
      }

      var sendIfAlreadyLocal = false;
      var payload = prepReadTargets(targets, this._subscriptions, cb, sendIfAlreadyLocal, this);

      var broadcaster = this.broadcaster;
      if ((! broadcaster.paused) && payload.length > 1) {
        broadcaster.emit('sub', payload);
      }
      return {id: payload[0]};
    }

    /**
     * @param {String|Query} targets[0]
     * @param {String|Query} targets[1]
     * ...
     * @param {Function} cb(err)
     */
  , unsubscribe: function (/* descriptors..., cb */) {
      var targets = Array.prototype.slice.call(arguments, 0);
      var cb;
      if (typeof targets[targets.length-1] === 'function') {
        cb = targets.pop();
        prepCallback(cb);
      }

      var subscriptions = this._subscriptions;
      var memory = this._memory;
      var uuid = this.id();
      var hashed = [uuid];

      for (var i = targets.length; i--; ) {
        var target = targets[i];
        var targetHash = hashable.hash(target);
        var metadata = subscriptions[targetHash];
        if (! metadata) {
          if (cb) cb.fulfill(targetHash, null);
          continue;
        }
        // TODO When to call cb
        metadata.unsubscribes.push(cb);
        if (--metadata.readCount === 0) {
          // TODO Make this cleanup more robust and generic
          memory.del(targetHash)
          delete memory._versions[targetHash];
          metadata.scopedResult.del();
          delete metadata.scopedResult;
          // TODO Remove metadata when we receive the 'ack.unsub'
        }
        hashed.push(hashable.hash(target));
      }
      if (hashed.length > 1) {
        this.broadcaster.emit('unsub', hashed);
      }
    }

    /**
     * @param {String|Query} targets[0]
     * @param {String|Query} targets[1]
     * ...
     */
  , unfetch: function (/* descriptors... */) {
      var targets = Array.prototype.slice.call(arguments, 0);
      var fetches = this._fetches;

      var memory = this._memory;
      var docReadCounts = this._docReadCounts;

      for (var i = targets.length; i--; ) {
        var target = targets[i];
        var targetHash = hashable.hash(target);
        var metadata = fetches[targetHash];
        var updatedCounts = readable.decrReadCount(target, docReadCounts, metadata);
        for (var docPath in updatedCounts) {
          if (updatedCounts[docPath] === 0) {
            memory.del(docPath);
            delete memory._versions[docPath];
          }
        }
        if (! metadata) continue;
        if (--metadata.readCount === 0) {
          // TODO Remove _$fetches.etc
          var scopedResult = metadata.scopedResult;
          if (scopedResult) {
            var resultPath = scopedResult.path();
            delete metadata.scopedResult;
            if (isPrivate(resultPath)) memory.del(resultPath);
          }
          delete fetches[targetHash];
        }
      }
    }

    /**
     * This callback reacts to incoming "ack.sub" events sent from the server
     * @param {Object} data represents the subscription results, from the server.
     * It looks like:
     *   {
     *     # Our snapshots and ops per document
     *     docs:
     *       <docPath>:
     *         snapshot:
     *           id: '...'
     *           attrOne: '...'
     *           ...
     *         ops: [
     *           txnOne
     *           txnTwo
     *         ]
     *       ...
     *     pointers:
     *       # Correspond to this.subscribe(targetA, targetB, targetC, cb)
     *       <resultPathA>: Boolean|Object
     *       <resultPathB>: {ns, id}
     *       <resultPathC>: {ns, ids}
     *   }
     * @param {Object} targetIndex maps target hashes to metadata about the
     * target and about its results, i.e., maps the hash of each target to:
     * {
     *   didReceiveSnapshot: Boolean
     * , scopedResult: ScopedModel
     * , target: String|Query
     * }
     */
  , _onReadAck: function (data, targetIndex) {
      var docs     = data.docs;
      var pointers = data.pointers;

      /* Handle the snapshots and ops encapsulated by docs */

      for (var docPath in docs) {
        var doc      = docs[docPath];
        var snapshot = doc.snapshot;
        var ops      = doc.ops;

        // Based on data from the server, update our snapshots and versions
        if (snapshot) {
          var ver = snapshot._v_;
          delete snapshot._v_;
          this._memory.set(docPath, snapshot, ver);
        }
        var versions = this._memory._versions;
        if (ops) for (var i = 0, l = ops.length; i < l; i++) {
          var txn = ops[i];
          var applyToVer = transaction.getVer(txn);
          var docPath = pathToDoc( transaction.getPath(txn) );
          if (applyToVer < versions[docPath]) continue;
          if (applyToVer === versions[docPath]) {
            // TODO Transform local ops against ops received from an "ack.sub"
            this[transaction.getMethod(txn)].apply(this, transaction.getArgs(txn));
            this._memory._versions[docPath] = applyToVer + 1;
          }
        }
      }

      /* Package up our results in scoped models associated with our
       * subscribe arguments
       */

      // TODO Handle when we un-subscribed before receiving this data
      for (var targetHash in pointers) {
        var ptr = pointers[targetHash];
        var readableMetadata = targetIndex[targetHash];
        var callbacks = readableMetadata.callbacks;
        var cb;
        while (cb = callbacks.shift()) {
          var scopedPath = readable.resultPath(readableMetadata.target, this);
          var scopedResult, keyPath;
          if (ptr === true) {
            scopedResult = this.at(pathUpToStar(targetHash));
          } else if ('id' in ptr) {
            // TODO Don't use substring
            keyPath = scopedPath.substring(0, scopedPath.length - '.results'.length) + '.id';
            this.set(keyPath, ptr.id);
            scopedResult = this.ref(scopedPath, ptr.ns, keyPath);
          } else if ('ids' in ptr) {
            // TODO Don't use substring
            keyPath = scopedPath.substring(0, scopedPath.length - '.results'.length) + '.ids';
            this.set(keyPath, ptr.ids);
            scopedResult = this.refList(scopedPath, ptr.ns, keyPath);
          }
          readableMetadata.scopedResult = scopedResult;
          cb.fulfill(targetHash, scopedResult);
        }
      }
    }

    /**
     * @param {String} targetHash
     * @param {String} err
     * @param {Object} targetIndex maps target hashes to metadata about the
     * target and about its results, i.e., maps the hash of each target to:
     * {
     *   didReceiveSnapshot: Boolean
     * , scopedResult: ScopedModel
     * , target: String|Query
     * }
     */
  , _onReadErr: function (targetHash, err, targetIndex) {
      var readableMetadata = targetIndex[targetHash];
      var callbacks = readableMetadata.callbacks;
      var cb;
      while (cb = callbacks.shift()) {
        cb(err);
        for (var k in cb) delete cb[k]; // GC
      }
    }

    // TODO Adapt and remove
  , _upstreamData: function (descriptors, cb) {
      if (!this.connected) return cb('disconnected');
      this.socket.emit('fetch', descriptors, this.scopedContext, cb);
    }

    // TODO Adapt and remove
  , _addSub: function (descriptors, cb) {
      if (! this.connected) return cb('disconnected');
      this.socket.emit('subscribe', descriptors, this.scopedContext, cb);
    }

    // TODO Adapt and remove
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

  , _allSubs: function () {
      var subs = []
        , types = this.descriptors
        , model = this;
      types.each( function (name, type) {
        subs = subs.concat(type.allSubs(model));
      });
      return subs;
    }

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

function serializedTargets (targetIndex) {
  var serialized = [];
  for (var targetHash in targetIndex) {
    serialized.push(
      serializable.serialize(targetIndex[targetHash].target)
    );
  }
  return serialized;
}

/**
 * @param {Array} targets
 * @param {Object} targetIndex
 * @param {Function} cb
 */
function prepReadTargets (targets, targetIndex, cb, sendIfAlreadyLocal, model) {
  if (cb) prepCallback(cb, targets);

  var toSend = [model.id()];
  for (var i = targets.length; i--; ) {
    var target = targets[i];
    var targetHash = hashable.hash(target);
    var readableMetadata;

    // If there are no pending or complete fetches/subscribes to the given target.
    if (! (targetHash in targetIndex)) {
      // If this is the first time in recent memory that we've subscribed
      // to the given target, then keep track of the target.
      readableMetadata = targetIndex[targetHash] = {
        callbacks: cb ? [cb] : []
      , target: target
      , unsubscribes: []
      , readCount: 1
      //, scopedResult
      };
      // And also add it to the subscription message we are going to send
      // over the wire to the server.
      var msg = readable.readMsg(target, model._fetches, model._subscriptions, model._memory._versions);
      toSend.push(msg);

    // If this subscribe includes a redundant subscription target that
    // co-incides with a prior subscribe, then...
    } else {
      readableMetadata = targetIndex[targetHash];

      // Count how many times we called fetch or subscribe on the target
      readableMetadata.readCount++;

      // Either we don't have a result from a prior read
      if (! readableMetadata.scopedResult) {
        // In this case we should use the prior read's future result
        // for the current read's result.
        if (cb) readableMetadata.callbacks.push(cb);

      // Or we do have a result from a prior read
      } else {
        // If we already have a local result, and this particular read wants to
        // send another explicit read anyways.
        if (sendIfAlreadyLocal) {
          if (cb) readableMetadata.callbacks.push(cb);
          toSend.push(readable.readMsg(target, model._fetches, model._subscriptions, model._memory._versions));
//          toSend.push({
//            t: serializable.serialize(target)
//          , m: readable.docMatches(target, model)
//          });
        } else {
          if (cb) cb.fulfill(targetHash, readableMetadata.scopedResult);
        }
      }
    }
    readable.maybeIncrReadCount(target, model._docReadCounts, readableMetadata);
  }
  return toSend;
}

/**
 * @param {Function} cb
 * @param {Array} targets
 */
function prepCallback (cb, targets) {
  // A callback, cb, is associated with the subscription targets with
  // which Model#subscribe(targets..., cb) is called.
  // When we receive subscription data from the server, we will keep
  // track of how many of of these targets have been received and only
  // invoke cb when received count equals expected count.
  cb.received = 0;
  cb.expected = targets.length;
  // Allocate space for the results of the subscribe.
  // The received target data will be turned into ScopedModel instances
  // and appended to cb.args, so we can cb.apply(null, cb.args) in the
  // future.
  cb.args = [null]; // maybe callback with null error

  for (var i = targets.length; i--; ) {
    var target = targets[i];
    var targetHash = hashable.hash(target);
    // So we can assign the received data to the proper callback
    // argument index later (see `incoming.on('ack.sub', fn)`)
    cb[targetHash] = i + 1; // offset by 1 because of err in fn(err, ...)
  }

  cb.fulfill = function (targetHash, scopedResult) {
    var argIndex = cb[targetHash];
    delete cb[targetHash];
    cb.args[argIndex] = scopedResult;
    if (++cb.received === cb.expected) {
      cb.apply(null, cb.args);
      delete cb.received;
      delete cb.expected;
      delete cb.args;
    }
  }
  return cb;
}
