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
      @emitter = emitStream @model.readStream

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
      @emitter = emitStream @model.readStream
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
      expect(callback).to.have.callCount(3)

  describe 'subscribe declaration streaming', ->
    beforeEach ->
      @model = new Model _clientId: 'x'
      @model.readStream.resume()
      @emitter = emitStream @model.readStream

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

  describe 'subscription acks', ->
    describe 'the first subscription', ->
      beforeEach ->
        @model = new Model _clientId: 'x'
        @model.readStream.resume()
        @remoteEmitter = new EventEmitter
        remoteStream = emitStream @remoteEmitter
        remoteStream.pipe @model.writeStream

      describe 'to a path target', ->
        beforeEach (done) ->
          called = false
          @model.subscribe 'collection.1', (err, @result) =>
            expect(err).to.equal null
            called = true
            done()
          expect(called).to.equal false
          @doc =
            id: 1
            name: 'Bryan'
            _v_: 0
          @remoteEmitter.emit 'ack.sub',
            docs:
              'collection.1':
                snapshot: @doc
            pointers:
              'collection.1': true
          expect(called).to.equal true

        it 'should callback with a scoped model', ->
          expect(@result).to.be.a(Model)
          expect(@result.path()).to.equal('collection.1')

        it 'should initialize the proper documents and versions', ->
          expect(@result.get()).to.eql @doc
          expect(@model.version('collection.1')).to.equal 0

    describe 'subsequent subscriptions', ->
      describe 'when overlapping result includes a doc at a later version', ->

  describe 'txn acks', ->
    beforeEach ->
      @model = new Model _clientId: 'x'
      @model.readStream.resume()
      @emitter = emitStream @model.readStream

    it 'should trigger the sending of any pending transactions', ->
      callback = sinon.spy()
      firstTxn = null
      @emitter.once 'txns', ([txn]) ->
        firstTxn = txn
        callback txn
      @model.set 'collection.1', a: 1, b: 2
      expect(callback).to.be.calledOnce()

      callback = sinon.spy()
      @emitter.on 'txns', callback
      @model.set 'collection.1.a', 4
      expect(callback).to.not.be.calledOnce()

      remoteEmitter = new EventEmitter
      remoteStream = emitStream remoteEmitter
      remoteStream.pipe @model.writeStream

      remoteEmitter.emit 'ack.txn', transaction.getId(firstTxn)
      expect(callback).to.be.calledOnce()

    describe 'timing out', ->
      it 'should re-send inflight transactions xxx', (done) ->
        @model.ackTimeout = 400
        callback = sinon.spy()
        firstTxn = null
        @emitter.once 'txns', ([txn]) ->
          firstTxn = txn
          callback txn
        @model.set 'collection.1', a: 1, b: 2
        expect(callback).to.be.calledOnce()
        @emitter.on 'txns', callback

        # Now, we suppose that for some reason the 'ack.txn' did not get
        # delivered to us. After some time, we would re-send the inflight
        # transaction.
        setTimeout ->
          expect(callback).to.be.calledTwice()
          done()
        , 800

#    describe '', ->
#      it 'should trigger the sending of any pending transactions', (done) ->
#        callback = sinon.spy()
#        firstTxn = null
#        @emitter.once 'txn', (txn) ->
#          firstTxn = txn
#          callback txn
#        @model.set 'collection.1', a: 1, b: 2
#        expect(callback).to.be.calledOnce()
#
#        callback = sinon.spy()
#        @emitter.on 'txn', callback
#        # Add a pending transaction
#        @model.set 'collection.1.a', 4
#        expect(callback).to.not.be.calledOnce()
#
#        remoteEmitter = new EventEmitter
#        remoteStream = emitStream remoteEmitter
#        remoteStream.pipe @model.writeStream
#
#        # Now, we suppose that for some reason the 'ack.txn' did not get
#        # delivered to us. After some time, we would re-send the inflight
#        # transaction.
#
#        remoteEmitter.emit 'ack.txn.dupe', transaction.getId(firstTxn)
#        expect(callback).to.be.calledOnce()

  describe 'incoming remote txns', ->
    describe 'when only subscribed to 1 target path', ->
      beforeEach (done) ->
        @model = new Model _clientId: 'x'
        @model.readStream.resume()
        @remoteEmitter = new EventEmitter
        remoteStream = emitStream @remoteEmitter
        remoteStream.pipe @model.writeStream
        @model.subscribe 'collection.1', (err, @result) =>
          expect(err).to.equal null
          done()
        @remoteEmitter.emit 'ack.sub',
          docs:
            'collection.1':
              snapshot:
                id: 1
                name: 'Bryan'
                _v_: 0
          pointers:
            'collection.1': true

      it 'should not be applied until after receiving a subscription snapshot'

      describe 'if the txn version is the next expected one', ->
        before ->
          @remoteTxn = transaction.create
            ver: 0
            method: 'set'
            args: ['collection.1.name', 'Brian']
            id: 'another-client.1'

        it 'should update the doc version', ->
          expect(@model.version('collection.1')).to.equal 0
          @remoteEmitter.emit 'txn', @remoteTxn
          expect(@model.version('collection.1')).to.equal 1

        it 'should mutate the document', ->
          expect(@result.get()).to.eql
            id: 1
            name: 'Bryan'
          @remoteEmitter.emit 'txn', @remoteTxn
          expect(@result.get()).to.eql
            id: 1
            name: 'Brian'

        it 'should emit mutation events on the model', ->
          fn = sinon.spy()
          @model.on 'set', 'collection.1.name', fn
          @remoteEmitter.emit 'txn', @remoteTxn
          expect(fn).to.be.calledOnce()

        it 'should be transformed against inflight and pending txns'

      # Test out of order remote transactions on a document
      describe 'if the txn version is not the next expected one', ->
        before ->
          @remoteTxn = transaction.create
            ver: 1
            method: 'set'
            args: ['collection.1.name', 'Brian']
            id: 'another-client.1'

        it 'should not update the doc version', ->
          expect(@model.version('collection.1')).to.equal 0
          @remoteEmitter.emit 'txn', @remoteTxn
          expect(@model.version('collection.1')).to.equal 0

        it 'should not mutate the document', ->
          expect(@result.get()).to.eql
            id: 1
            name: 'Bryan'
          @remoteEmitter.emit 'txn', @remoteTxn
          expect(@result.get()).to.eql
            id: 1
            name: 'Bryan'

        describe 'then subsequently receiving a missing txn', ->
          beforeEach ->
            @remoteEmitter.emit 'txn', @remoteTxn
            @missingTxn = transaction.create
              ver: 0
              method: 'set'
              args: ['collection.1.height', 6]

          it 'should update the doc version', ->
            @remoteEmitter.emit 'txn', @missingTxn
            expect(@model.version('collection.1')).to.equal 2

          it 'should mutate the doc', ->
            @remoteEmitter.emit 'txn', @missingTxn
            expect(@result.get()).to.eql
              id: 1
              name: 'Brian'
              height: 6

    describe 'when subscribed to 2+ targets', ->
      it 'should not be applied if the txn version is not the next expected one'

      # TODO We won't know what txns we might receive for certain queries, ahead of time
      describe 'the txn version is the next expected one', ->
        # If we apply the version update to universe, then we might miss
        # server-side transforming future txn against the mutation associated with
        # this txn
        describe 'ver-only txns', ->
          it 'should be cached but not applied if no equiv ver-only txns have been received'
          it 'should be uncached if it receives the next txn with ver + mutation info'
          it 'should be ignored if an equiv ver-only txn has been received for another subscription'
        describe 'ver + mutation txns', ->
          it 'should be applied immediately'
          it 'should be ignored if equiv txns have been received for another subscription'
