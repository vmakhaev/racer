/**
 * TransactionRegistry keeps track of transactions generated locally and
 * transactions received from the server.
 */

var EventEmitter = require('events').EventEmitter
  , transaction = require('./transaction')
  , pathToDoc = require('./path').pathToDoc
  ;

module.exports = TransactionRegistry;

function TransactionRegistry (idGenerator) {
  this.nextId = idGenerator;
  this._txns = {}; // transaction id -> transaction
  this._txnQueueByDoc = {}; // "#{ns}.{doc.id}" -> [transactionIds...]
  this._localTxnQueue = []; // [transactionIds...]
  EventEmitter.call(this);
}

require('util').inherits(TransactionRegistry, EventEmitter);

// TODO How will this work with multi-doc transactions?
TransactionRegistry.prototype.add = function (txn) {
  var txnId = transaction.getId(txn);
  this._txns[txnId] = txn;
  this._localTxnQueue.push(txnId);

  this._matchingDocTxnQueue(txn).push(txnId);
};

TransactionRegistry.prototype._matchingDocTxnQueue = function (txn) {
  var path = transaction.getArgs(txn)[0]
    , docPath = pathToDoc(path)
    , txnsByDoc = this._txnQueueByDoc;
  return txnsByDoc[docPath] || (txnsByDoc[docPath] = []);
};

TransactionRegistry.prototype.removeById = function (txnId) {
  var txn = this._txns[txnId];
  delete this._txns[txnId];
  var queue = this._localTxnQueue;
  removeFromArray(this._localTxnQueue, txnId);
  removeFromArray(this._matchingDocTxnQueue(txn), txnId);
  this.emit('remove');
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
