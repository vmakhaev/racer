emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model txn', ->
  describe 'acks', ->
    beforeEach ->
      @model = new Model _clientId: 'x'
      @model.readStream.resume()
      @emitter = emitStream @model.readStream

    afterEach ->
      @emitter.removeAllListeners()

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
      remoteEmitter.removeAllListeners()
      expect(callback).to.be.calledOnce()

    it 'should re-send inflight txns at intervals until receiving "ack.txn"', (done) ->
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

    describe 'ack indicating the txn has already been applied', ->
      describe "when ack'ed txn is still in pending", ->
        it 'should trigger the sending of any pending transactions', ->
          callback = sinon.spy()
          firstTxn = null
          @emitter.once 'txns', ([txn]) ->
            firstTxn = txn
            callback()
          @model.set 'collection.1', a: 1, b: 2
          expect(callback).to.be.calledOnce()

          callback = sinon.spy()
          @emitter.on 'txns', callback
          # Add a pending transaction
          @model.set 'collection.1.a', 4
          expect(callback).to.not.be.calledOnce()

          remoteEmitter = new EventEmitter
          remoteStream = emitStream remoteEmitter
          remoteStream.pipe @model.writeStream

          # Now, we suppose that for some reason the 'ack.txn' did not get
          # delivered to us. After some time, we would re-send the inflight
          # transaction.

          # The server would know that the txns were applied, so it would
          # inform the client that the txns are duplicates
          remoteEmitter.emit 'ack.txn.dupe', transaction.getId(firstTxn)
          remoteEmitter.removeAllListeners()
          expect(callback).to.be.calledOnce()

      describe "when ack'ed txn is no longer in pending (i.e., received a prior ack)", ->
        it 'should not trigger the sending of any pending transactions', ->
          callback = sinon.spy()
          firstTxn = null
          @emitter.once 'txns', ([txn]) ->
            firstTxn = txn
            callback()
          @model.set 'collection.1', a: 1, b: 2
          expect(callback).to.be.calledOnce()

          subsequentCallback = sinon.spy()
          @emitter.on 'txns', subsequentCallback
          # Add a pending transaction
          @model.set 'collection.1.a', 4
          expect(subsequentCallback).to.not.be.calledOnce()

          remoteEmitter = new EventEmitter
          remoteStream = emitStream remoteEmitter
          remoteStream.pipe @model.writeStream

          remoteEmitter.emit 'ack.txn', transaction.getId(firstTxn)
          expect(subsequentCallback).to.be.calledOnce()

          # Now, we suppose that for some reason the 'ack.txn.dupe' also is
          # delivered to us.

          # We'll ignore it because we already received an 'ack.txn'
          remoteEmitter.emit 'ack.txn.dupe', transaction.getId(firstTxn)
          remoteEmitter.removeAllListeners()
          expect(callback).to.have.callCount(1)

  describe 'incoming remote txns', ->
    describe 'when only subscribed to 1 target path', ->
      beforeEach (done) ->
        @model = new Model _clientId: 'x'
        @model.readStream.resume()
        @remoteEmitter = new EventEmitter
        remoteStream = emitStream @remoteEmitter
        remoteStream.pipe @model.writeStream
        {id} = @model.subscribe 'collection.1', (err, @result) =>
          expect(err).to.equal null
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

      afterEach ->
        @remoteEmitter.removeAllListeners()

      describe 'before receiving initial subscription snapshots', ->
        it 'should not be applied'

      describe "if initial subscription snapshots don't include the relevant doc", ->
        it 'should not be applied'

        describe 'but later we receive an addDoc', ->
          it 'should be applied'

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
