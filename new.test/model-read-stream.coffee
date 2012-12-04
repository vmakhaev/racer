emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model read stream', ->
  it 'should stream transactions', ->
    model = new Model
    model._clientId = 'x'
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1', a: 1, b: 2
    expect(callback).to.be.called

  it 'should not stream private path transactions', ->
    model = new Model
    model._clientId = 'x'
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1._temporary', 'x'
    expect(callback).to.not.be.called

  it "should not stream a txn if the relevant doc already has an inflight txn (i.e., unack'ed)", ->
    model = new Model
    model._clientId = 'x'
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1', a: 1, b: 2
    model.set 'collection.1.a', 3
    expect(callback).to.be.calledOnce()

  it 'should stream a txn if no doc-related inflight txns, even if other docs have inflight txns', ->
    model = new Model
    model._clientId = 'x'
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1', a: 1, b: 2
    model.set 'collection.2', a: 1, b: 2
    expect(callback).to.be.calledTwice()

  it "should stream a pending txn once all inflight txns for the same doc have been ack'ed", ->
    model = new Model
    model._clientId = 'x'
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1', a: 1, b: 2
    model.set 'collection.1.a', 3

    remoteEmitter = new EventEmitter
    remoteStream = emitStream remoteEmitter
    remoteStream.pipe model.writeStream

    remoteEmitter.emit 'txnOk', transaction.create(id: 'x.0', method: 'set', args: ['collection.1', {a: 1, b: 2}])
    expect(callback).to.be.calledTwice()
