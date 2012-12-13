emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model read stream', ->
  beforeEach ->
    @model = new Model _clientId: 'x'

  it 'should be paused by default', ->
    expect(@model.readStream.paused).to.be.ok()

  describe 'transaction streaming', ->
    beforeEach ->
      @model.readStream.resume()
      @emitter = @model.broadcaster

    afterEach ->
      @emitter.removeAllListeners()

    it 'should stream transactions', ->
      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1', a: 1, b: 2
      expect(callback).to.be.calledOnce()

    it 'should not stream private path transactions', ->
      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1._temporary', 'x'
      expect(callback).to.have.callCount(0)

    it "should not stream a txn if the relevant doc already has an inflight txn (i.e., unack'ed)", ->
      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1', a: 1, b: 2
      @model.set 'collection.1.a', 3
      expect(callback).to.be.calledOnce()

    it 'should stream a txn if no doc-related inflight txns, even if other docs have inflight txns', ->
      @emitter = @model.broadcaster
      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1', a: 1, b: 2
      @model.set 'collection.2', a: 1, b: 2
      expect(callback).to.be.calledTwice()

    it "should stream a pending txn once all inflight txns for the same doc have been ack'ed", ->
      callback = sinon.spy()
      firstTxn = null
      @emitter.on 'txns', ([txn]) ->
        firstTxn ||= txn
        callback(txn)
      @model.set 'collection.1', a: 1, b: 2
      @model.set 'collection.1.a', 3

      remoteEmitter = new EventEmitter
      remoteStream = emitStream remoteEmitter
      remoteStream.pipe @model.writeStream

      remoteEmitter.emit 'ack.txn', transaction.getId(firstTxn)
      remoteEmitter.removeAllListeners()
      expect(callback).to.be.calledTwice()

    it 'should re-send an inflight txn if the stream is resumed (i.e., unpaused), and an inflight txn is on record', ->
      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1', a: 1, b: 2
      @model.readStream.pause()
      @model.readStream.resume()
      expect(callback).to.be.calledTwice()

    it 'should send pending txns if the stream is resumed (i.e., unpaused), after re-sending and acking inflight txns', ->
      callback = sinon.spy()
      firstTxn = null
      @emitter.on 'txns', ([txn]) ->
        firstTxn ||= txn
        callback txn
      @model.set 'collection.1', a: 1, b: 2
      @model.set 'collection.1.a', 3
      @model.readStream.pause()
      @model.readStream.resume()

      remoteEmitter = new EventEmitter
      remoteStream = emitStream remoteEmitter
      remoteStream.pipe @model.writeStream

      remoteEmitter.emit 'ack.txn', transaction.getId(firstTxn)
      remoteEmitter.removeAllListeners()
      expect(callback).to.have.callCount(3)

  describe 'subscribe declaration streaming', ->
    beforeEach ->
      @model = new Model _clientId: 'x'
      @model.readStream.resume()
      @emitter = @model.broadcaster

    it 'should stream subscribes', ->
      callback = sinon.spy()
      @emitter.on 'sub', callback
      @model.subscribe 'collection.*.name'
      expect(callback).to.be.calledOnce()

    it 'should re-send subscriptions if the stream is resumed (i.e., unpaused)', ->
      callback = sinon.spy()
      @emitter.on 'sub', callback
      @model.subscribe 'collection.*.name'
      @model.readStream.pause()
      @model.readStream.resume()
      expect(callback).to.have.callCount(2)
