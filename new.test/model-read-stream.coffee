emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model read stream', ->
  it 'should be paused by default', ->
    model = new Model
    expect(model.readStream.paused).to.be.ok()

  describe 'transaction streaming', ->
    it 'should stream transactions', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1', a: 1, b: 2
      expect(callback).to.be.calledOnce()

    it 'should not stream private path transactions', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1._temporary', 'x'
      expect(callback).to.have.callCount(0)

    it "should not stream a txn if the relevant doc already has an inflight txn (i.e., unack'ed)", ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1', a: 1, b: 2
      model.set 'collection.1.a', 3
      expect(callback).to.be.calledOnce()

    it 'should stream a txn if no doc-related inflight txns, even if other docs have inflight txns', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1', a: 1, b: 2
      model.set 'collection.2', a: 1, b: 2
      expect(callback).to.be.calledTwice()

    it "should stream a pending txn once all inflight txns for the same doc have been ack'ed", ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
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

    it 'should re-send an inflight txn if the stream is resumed (i.e., unpaused), and an inflight txn is on record', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1', a: 1, b: 2
      model.readStream.pause()
      model.readStream.resume()
      expect(callback).to.be.calledTwice()

    it 'should send pending txns if the stream is resumed (i.e., unpaused), after re-sending and acking inflight txns', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'txn', callback
      model.set 'collection.1', a: 1, b: 2
      model.set 'collection.1.a', 3
      model.readStream.pause()
      model.readStream.resume()

      remoteEmitter = new EventEmitter
      remoteStream = emitStream remoteEmitter
      remoteStream.pipe model.writeStream

      remoteEmitter.emit 'txnOk', transaction.create(id: 'x.0', method: 'set', args: ['collection.1', {a: 1, b: 2}])
      expect(callback).to.have.callCount(3)

  describe 'subscribe declaration streaming', ->
    it 'should stream subscribes', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'sub', callback
      model.subscribe 'collection.*.name'
      expect(callback).to.be.calledOnce()

    it 'should re-send subscriptions if the stream is resumed (i.e., unpaused)', ->
      model = new Model
      model._clientId = 'x'
      model.readStream.resume()
      emitter = emitStream model.readStream
      callback = sinon.spy()
      emitter.on 'sub', callback
      model.subscribe 'collection.*.name'
      model.readStream.pause()
      model.readStream.resume()
      expect(callback).to.have.callCount(2)
