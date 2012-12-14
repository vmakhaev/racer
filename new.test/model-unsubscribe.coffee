emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model unsubscribe', ->
  beforeEach ->
    @model = new Model _clientId: 'x'
    @emitter = @model.broadcaster
    @remoteEmitter = new EventEmitter
    remoteStream = emitStream @remoteEmitter
    remoteStream.pipe @model.writeStream

  afterEach ->
    @emitter.removeAllListeners()
    @remoteEmitter.removeAllListeners()

  describe 'when not subscribed to the target at all', ->
    it 'should not send a message to the server', ->
      cb = sinon.spy()
      @emitter.on 'unsub', cb
      @model.unsubscribe 'collection.1'
      expect(cb).to.have.callCount(0)

  describe 'after a subscribe', ->
    beforeEach (done) ->
      {id} = @model.subscribe 'collection.1', (err, @result) =>
        done()
      @remoteEmitter.emit 'ack.sub',
        id: id
        docs:
          'collection.1':
            snapshot:
              id: 1
              name: 'Bryan'
              _v_: 0
        pointers:
          'collection.1': true

    it 'should send a message to the server at least once', ->
      cb = sinon.spy()
      @emitter.on 'unsub', cb
      @model.unsubscribe 'collection.1'
      expect(cb).to.be.calledOnce()

    # Otherwise, the server will over-publish events to the browser
    it 'should keep on sending unsubscribes at intervals until receiving "ack.unsub"', ->
      cb = sinon.spy()
      @emitter.on 'unsub', cb
      @model.unsubscribe 'collection.1'
      setTimeout ->
        expect(cb).to.be.calledTwice()
      , 400 + 200

    it 'should clean up data that belongs to no other fetches or subscriptions, before an ack', ->
      expect(@model.get('collection.1')).to.not.equal undefined
      @model.unsubscribe 'collection.1'
      expect(@model.get('collection.1')).to.equal undefined

    describe 'after a duplicate subscribe', ->
      beforeEach (done) ->
        @model.subscribe 'collection.1', (err) -> done()
