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

function TransactionRegistry (idGenerator) {
  this.nextId = idGenerator;
  this._txns = {}; // transaction id -> transaction
  this._pendingByDoc = {}; // "#{ns}.{doc.id}" -> [transactionIds...]
  this._inflightByDoc = {}; // "#{ns}.{doc.id}" -> [transactionIds...]
  EventEmitter.call(this);
}

require('util').inherits(TransactionRegistry, EventEmitter);

TransactionRegistry.prototype.addPending = function (txn) {
  var txnId = transaction.getId(txn);
  this._txns[txnId] = txn;
  this._matchingPending(txn).push(txnId);
};

TransactionRegistry.prototype.readyToEmit = function (txn) {
  var inflight = this._matchingInflight(txn);
  return !(inflight && inflight.length);
};

TransactionRegistry.prototype.pendingToInflight = function (txn) {
  var pendingTxns = [];
  var txns = this._txns;
  var pendingIds = this._matchingPending(txn);
  var inflightIds = this._matchingInflight(txn);
  var txnId;
  while (txnId = pendingIds.shift()) {
    inflightIds.push(txnId);
    pendingTxns.push(txns[txnId]);
  }
  return pendingTxns;
};

TransactionRegistry.prototype.eachInflight = function (cb) {
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

TransactionRegistry.prototype._matchingQueue = function (txn, type) {
  var path = transaction.getArgs(txn)[0]
    , docPath = pathToDoc(path)
    , txnsByDoc = this['_' + type + 'ByDoc'];
  return txnsByDoc[docPath] || (txnsByDoc[docPath] = []);
};

TransactionRegistry.prototype.ack = function (txn) {
  var pendingIds = this._matchingInflight(txn);
  var txnId = transaction.getId(txn);
  pendingIds.splice(pendingIds.indexOf(txnId), 1);
  delete this._txns[txnId];
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
