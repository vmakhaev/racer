/**
 * TransactionRegistry keeps track of transactions generated locally and
 * transactions received from the server.
 */

// TODO How will this work with multi-doc transactions?
// TODO Cleanup un-used methods here

var EventEmitter = require('events').EventEmitter
  , transaction = require('./transaction')
  , pathToDoc = require('./path').pathToDoc
  ;

module.exports = TransactionRegistry;

/**
 * @param {Function} idGenerator
 */
function TransactionRegistry (idGenerator) {
  this.nextId = idGenerator;
  this._txns = {}; // transaction id -> transaction
  this._pendingByDoc = {}; // "#{ns}.{doc.id}" -> [transactionIds...]
  this._inflightByDoc = {}; // "#{ns}.#{doc.id}" -> [transactionIds...]
  this._remoteByDoc = {}; // #{ns}.#{doc.id}"
  EventEmitter.call(this);
}

require('util').inherits(TransactionRegistry, EventEmitter);

TransactionRegistry.prototype.addPending = function (txn) {
  var txnId = transaction.getId(txn);
  this._txns[txnId] = txn;
  this._matchingPending(txn).push(txnId);
};

TransactionRegistry.prototype.addRemote = function (txn) {
  var txnId = transaction.getId(txn);
  this._txns[txnId] = txn;
  this._matchingRemote(txn).push(txnId);
};

TransactionRegistry.prototype.eachSequentialRemote = function (afterTxn, cb) {
  var remoteIds = this._matchingRemote(afterTxn);
  if (! remoteIds.length) return;
  var afterVer = transaction.getVer(afterTxn);
  var txns = this._txns;
  for (var i = 0, l = remoteIds.length; i < l; i++) {
    var remoteTxn = txns[remoteIds[i]]
    var ver = transaction.getVer(remoteTxn);
    if (ver === ++afterVer) {
      cb(remoteTxn);
    } else {
      return;
    }
  }
};

TransactionRegistry.prototype.removeRemote = function (txn) {
  var txnId = transaction.getId(txn);
  delete this._txns[txnId];
  var remoteIds = this._matchingRemote(txn);
  remoteIds.splice(remoteIds.indexOf(txnId));
  if (! remoteIds.length) {
    var path = transaction.getPath(txn);
    delete this._remoteByDoc[pathToDoc(path)];
  }
};

TransactionRegistry.prototype.readyToEmit = function (txn) {
  if (! Array.isArray(txn)) {
    txn = this._txns[txn]; // txn is txnId
  }
  var inflight = this._matchingInflight(txn);
  return !(inflight && inflight.length);
};

TransactionRegistry.prototype.pendingToInflight = function (txn) {
  var pendingTxns = [];
  var txns = this._txns;
  if (! Array.isArray(txn)) {
    txn = txns[txn]; // txn is txnId
  }
  var pendingIds = this._matchingPending(txn);
  var inflightIds = this._matchingInflight(txn);
  var pendingTxnId;
  while (pendingTxnId = pendingIds.shift()) {
    inflightIds.push(pendingTxnId);
    pendingTxns.push(txns[pendingTxnId]);
  }
  return pendingTxns;
};

TransactionRegistry.prototype.eachInflightsByDoc = function (cb) {
  var inflights = this._inflightByDoc
    , txns = this._txns
    , txnIds
    , txnList;
  for (var pathToDoc in inflights) {
    txnIds = inflights[pathToDoc];
    if (! txnIds.length) continue;
    txnList = [];
    for (var i = 0, l = txnIds.length; i < l; i++) {
      txnList.push(txns[txnIds[i]]);
    }
    cb(txnList, pathToDoc);
  }
};

TransactionRegistry.prototype._matchingInflight = function (txn) {
  return this._matchingQueue(txn, 'inflight');
};

TransactionRegistry.prototype._matchingPending = function (txn) {
  return this._matchingQueue(txn, 'pending');
};

TransactionRegistry.prototype._matchingRemote = function (txn) {
  return this._matchingQueue(txn, 'remote');
};

TransactionRegistry.prototype._matchingQueue = function (txn, type) {
  var path = transaction.getArgs(txn)[0]
    , docPath = pathToDoc(path)
    , txnsByDoc = this['_' + type + 'ByDoc'];
  return txnsByDoc[docPath] || (txnsByDoc[docPath] = []);
};

TransactionRegistry.prototype.ack = function (txnId) {
  var txn = this._txns[txnId];

  // This could happen if we sent the inflight transactions to the server
  // more than once. Then we would receive a 'ack.txn' and 'ack.txn.dupe',
  // which would both invoke this.ack(txnId). By the time we receive the
  // second message, we'll already have a new set of inflight transactions,
  // so we can ignore the second message.
  if (! txn) return false;

  var inflightIds = this._matchingInflight(txn);
  var lastInflightId = inflightIds[inflightIds.length - 1]
  if (lastInflightId === txnId) {
    for (var i = inflightIds.length; i--; ) {
      delete this._txns[inflightIds[i]];
    }
    inflightIds.length = 0;
    return this.pendingToInflight(txn);
  } else {
    console.error('This should not happen.');
    return false;
  }
};

TransactionRegistry.prototype.removeById = function (txnId) {
  var txn = this._txns[txnId];
  delete this._txns[txnId];
  removeFromArray(this._matchingQueue(txn), txnId);
};

TransactionRegistry.prototype.remove = function (txn) {
  var txnId = transaction.getId(txn);
  if (txnId) this.removeById(txnId);
};

TransactionRegistry.prototype.eachLocal = function (cb) {
  var q = this._localTxnQueue;
  for (var i = 0, l = q.length; i < l; i++) {
    cb(q[i]);
  }
};

// TODO
// We'll really only know what's applicable if...
TransactionRegistry.prototype.applicable = function (cb) {
  var queues = this._txnQueueByDoc
    , q;
  for (var pathToDoc in queues) {
    q = queues[pathToDoc];
  }
};

function removeFromArray (xs, x) {
  var i = xs.indexOf(x);
  if (~i) xs.splice(i, 1);
}
