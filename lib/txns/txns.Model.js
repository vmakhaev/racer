var TransactionRegistry = require('../TransactionRegistry')
  , Serializer = require('../Serializer')
  , transaction = require('../transaction')
  , pathUtils = require('../path')
  , isPrivate = pathUtils.isPrivate
  , pathToDoc = pathUtils.pathToDoc
  , createMiddleware = require('../middleware')

    // Timeout in milliseconds after which sent transactions will be resent
  , SEND_TIMEOUT = 20000

    // Interval in milliseconds to check timeouts for queued transactions
  , RESEND_INTERVAL = 20000
  ;

module.exports = {
  type: 'Model'

, static: {
    SEND_TIMEOUT: SEND_TIMEOUT
  , RESEND_INTERVAL: RESEND_INTERVAL
  }

, events: {
    // TODO Think through situation when we send 'txn' but 'txnOk' is sent
    // while the model is in flight to the browser
    init: function (model) {
      this._ops = {
        // <pathTodoc>: {
        //   pending: []
        // , inflight: null
        // , server: []
        // }
        //
      };

      var createdCount = 0;
      function idGenerator () {
        return model._clientId + createdCount++;
      }
      var registry = model._txnRegistry = new TransactionRegistry(idGenerator);

      model.readStream.on('drain', function () {
        registry.eachInflight( function (txns) {
          for (var i = 0, l = txns.length; i < l; i++) {
            broadcaster.emit('txn', txns[i]);
          }
        });
      });

      var broadcaster = model.broadcaster;
      var incoming = model.incoming;
      incoming.on('txn', function (txn) {
        // Update the version, appropriately
        var ver = transaction.getVer(txn);
        var args = transaction.getArgs(txn);
        var path = args[0];
        var docPath = pathToDoc(path);
        var versions = model._memory._versions;
        var currVer = versions[docPath];
        if (ver < currVer) return;
        if (ver > currVer) {
          return; // TODO
        }
        versions[docPath] = ver+1;
        var method = transaction.getMethod(txn);
        // Mutate the document
        model[method].apply(model, args);
      });
      incoming.on('ack.txn', function (txnId) {
        var readyToEmit = registry.ack(txnId);
        if (readyToEmit) {
          var pending = readyToEmit;
          for (var i = 0, l = pending.length; i < l; i++) {
            broadcaster.emit('txn', pending[i]);
          }
        }
      });

      return

      // TODO Adapt and remove
      model._onTxn = function (txn) {
        if (!txn) return;

        // Copy meta properties onto this txn if it matches one in the queue
        var txnQ = model._txns[transaction.getId(txn)];
        if (txnQ) {
          txn.callback = txnQ.callback;
          txn.emitted = txnQ.emitted;
        }

        var isLocal = 'callback' in txn
          , ver = transaction.getVer(txn);
        if (ver > memory.version || ver === -1) {
          model._applyTxn(txn, isLocal);
        }
      };
    }

  , middleware: function (_model, middleware) {
      // TODO Replace middleware with events
      middleware.txn = createMiddleware();

      middleware.txn.add(normalizeTxn);
      // TODO middleware.txn.add(typecast);
      // TODO middleware.txn.add(validate);
      // Evaluate the transaction to create a new snapshot
      middleware.txn.add(evalTxn);
      // Add insert index as txn metadata
      middleware.txn.add(addInsertMetadata);
      // Log our txn
      middleware.txn.add(logTxn);
      // Sending txns to server must happen before emit, since emissions might create
      // other transactions as a side effect, that would be sent up before this one.
      middleware.txn.add(maybeBroadcastTxn);
//      middleware.txn.add(commitTxn);
      // Send the transaction ...
      // - ... over Socket.IO if a browser Model
      // - ... to the store if a server Model
      middleware.txn.add(emitTxn);

      function normalizeTxn (req, res, next) {
        var txn = req.data
          , method = transaction.getMethod(txn)
          , args = transaction.getArgs(txn)
          , model = req.model;

        // Refs may mutate the args in its 'beforeTxn' handler
        model.emit('beforeTxn', method, args);
        var path = args[0];
        if (typeof path === 'undefined') return;
        txn.isPrivate = isPrivate(path);

        txn.emitted = args.cancelEmit;

        // Add remove index as txn metadata. Null if transaction does nothing
        if (method === 'pop') {
          var arr = model.get(path);
          txn.push(arr ? arr.length - 1 : null);
        } else if (method === 'unshift') {
          txn.push(model.get(path) ? 0 : null);
        }
        return next();
      }

      function evalTxn (req, res, next) {
        var txn = req.data;
        req.model._applyTxn(txn, false);
        // TODO res.out = ...
        // res.out = req.model._specModel().$out;
        return next();
      }

      function addInsertMetadata (req, res, next) {
        var txn = req.data;
        if (txn.isPrivate) return next();
        var args = transaction.getArgs(txn)
          , method = transaction.getMethod(txn)
          ;
        if (method === 'push') {
          var out = res.out
            , k = out - args.length + 1;
          transaction.setMeta(k);
          txn.push(k);
        }
        return next();
      }

      function logTxn (req, res, next) {
        var txn = req.data;
        req.model.logTxn(txn);
        return next();
      }

      function maybeBroadcastTxn (req, res, next) {
        var txn = req.data;
        if (txn.isPrivate) return next();
        var model = req.model;
        if (model._txnRegistry.readyToEmit(txn)) {
          var pending = model._txnRegistry.pendingToInflight(txn);
          for (var i = 0, l = pending.length; i < l; i++) {
            model.broadcaster.emit('txn', pending[i]);
          }
        }
        return next();
      }

      // TODO Remove
      function commitTxn (req, res, next) {
        var txn = req.data;
        req.model._commit(txn);
        return next();
      }

      function emitTxn (req, res, next) {
        var txn = req.data;
        if (txn.emitted) return res.out;

        var method = transaction.getMethod(txn)
            // Clone the args, so that they can be modified before being
            // emitted without affecting the txn args
          , args = transaction.copyArgs(txn)
          , model = req.model;
        model.emit(method, args, res.out, true, model._pass);
        txn.emitted = true;
        // return next();
        return res.out;
      }
    }

    // TODO Timeout for receiving expected txns
  , socket: function (model, socket) {
      var memory    = model._memory
        , removeTxn = model._removeTxn
        , onTxn     = model._onTxn

      var txnApplier = new Serializer({
        withEach: onTxn

        // This timeout is for scenarios when a service that the server proxies
        // to fails. This is for remote transactions.
      , onTimeout: function () {
          // TODO Make sure to set up the timeout again if we are disconnected
          if (! model.connected) return;
          // TODO Don't do this if we are also responding to a resyncWithStore
          socket.emit('fetch:snapshot', memory.version + 1, model._startId, model._subs());
        }
      });

      function resend () {
        var now = +new Date;
        var txns = model._txns
          , txnQueue = model._txnQueue
        for (var i = 0, l = txnQueue.length; i < l; i++) {
          var id = txnQueue[i]
            , txn = txns[id];
          if (! txn || txn.timeout > now) return;
          commit(txn);
        }
      }

      // Set an interval to check for transactions that have been in the queue
      // for too long and resend them
      var resendInterval = null;
      function setupResendInterval () {
        if (!resendInterval) resendInterval = setInterval(resend, RESEND_INTERVAL);
      }

      function teardownResendInterval () {
        if (resendInterval) clearInterval(resendInterval);
        resendInterval = null;
        if (model.connected) {
          setupResendInterval();
        } else {
          model.once('connect', setupResendInterval);
        }
      }

      // Stop resending transactions until reconnect
      // TODO Stop asking for missed remote transactions until reconnect
      socket.on('disconnect', teardownResendInterval);
      teardownResendInterval();

      model._addRemoteTxn = addRemoteTxn;
      function addRemoteTxn (txn, num) {
        if (typeof num !== 'undefined') {
          txnApplier.add(txn, num);
        } else {
          onTxn(txn);
        }
      }

      socket.on('txn', addRemoteTxn);

      // The model receives 'txnOk' from the server/store after the
      // server/store applies a transaction that originated from this model successfully
      socket.on('txnOk', function (rcvTxn, num) {
        var txnId = transaction.getId(rcvTxn)
          , txn = model._txns[txnId];
        if (!txn) return;
        var ver = transaction.getVer(rcvTxn);
        transaction.setVer(txn, ver);
        addRemoteTxn(txn, num);
      });

      // The model receives 'txnErr' from the server/store after the
      // server/store attempts to apply this transaction but fails
      socket.on('txnErr', function (err, txnId) {
        var txn = model._txns[txnId]
          , callback = txn && txn.callback;
        removeTxn(txnId);
        if (callback) {
          var callbackArgs = (transaction.isCompound(txn))
                           ? transaction.ops(txn)
                           : transaction.copyArgs(txn);
          callbackArgs.unshift(err);
          callback.apply(null, callbackArgs);
        }
      });

      model._commit = commit;
      function commit (txn) {
        if (txn.isPrivate) return;
        txn.timeout = +new Date + SEND_TIMEOUT;

        // Don't queue this up in socket.io's message buffer. Instead, we
        // explicitly send over an txns in this_txnQueue during reconnect synchronization
        if (! model.connected) return;

        socket.emit('txn', txn, model._startId);
      }
    }
  }

, server: {
    _commit: function (txn) {
      if (txn.isPrivate) return;
      var self = this
        , req = {
            data: txn
          , ignoreStartId: true
          , clientId: this._clientId
          , session: this.session
          }
        , res = {
            fail: function (err, txn) {
              self._removeTxn(transaction.getId(txn));
              txn.callback(err, txn);
            }
          , send: function (txn) {
              self._onTxn(txn);
              self.store.serialCleanup(txn);
            }
          };
      this.store.middleware.txn(req, res);
    }
  }

, proto: {
    _commit: function () {}
  , _asyncCommit: function (txn, cb) {
      if (! this.connected) return cb('disconnected');
      txn.callback = cb;
      var id = transaction.getId(txn);
      this._txns[id] = txn;
      this._commit(txn);
    }

    // TODO Remove?
  , _queueTxn: function (txn, cb) {
      txn.callback = cb;
      var id = transaction.getId(txn);
      this._txns[id] = txn;
      this._txnQueue.push(id);
    }

  , version: function (path) {
      return this._memory.version(path);
    }

  , logTxn: function (txn) {
      this._txnRegistry.addPending(txn);
    }

  , _opToTxn: function (method, args, cb) {
      var ver = this.version(args[0])
        , id = this._txnRegistry.nextId()
        , txn = transaction.create({
            ver: ver
          , id: id
          , method: method
          , args: args})
        ;
      txn.callback = cb;
      return txn;
    }

  , _sendToMiddleware: function (method, args, cb) {
      var txn = this._opToTxn(method, args, cb)
        , req = {
            data: txn
            // Pass in model, just in case scoped model where we need to access
            // model._pass
          , model: this
          }
        , res = {
            fail: function (err) { throw err; }
          , send: function () { console.log('TODO'); }
          }
        ;
      return this.middleware.txn(req, res);
    }

  , _applyTxn: function (txn, isLocal) {
      var data = this._memory._data
        , doEmit = !txn.emitted
          // TODO Do we need Math.floor anymore?
        , ver = Math.floor(transaction.getVer(txn))
        , isCompound = transaction.isCompound(txn)
        , out
        ;
      // TODO Support compound txns
      out = this._applyMutation(transaction, txn, ver, data, doEmit, isLocal);

      var callback = txn.callback;
      if (callback) {
        if (isCompound) {
          callback.apply(null, [null].concat(transaction.ops(txn)));
        } else {
          callback.apply(null, [null].concat(transaction.getArgs(txn), out));
        }
      }
      return out;
    }

    // `extractor` is either `transaction` or `transaction.op`
  , _applyMutation: function (extractor, txn, ver, data, doEmit, isLocal) {
      var out = extractor.applyTxn(txn, data, this._memory, ver);
      if (doEmit) {
        var method = transaction.getMethod(txn)
          , args = transaction.getArgs(txn);
        this.emit(method, args, out, isLocal, this._pass);
        txn.emitted = true;
      }
      return out;
    }
  }
};
