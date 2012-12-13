emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

# TODO Test that subsequent duplicate subscription targets do not send a second
# time over the wire.

describe 'Model subscribe', ->
  beforeEach ->
    @model = new Model _clientId: 'x'
    @emitter = @model.broadcaster
    @remoteEmitter = new EventEmitter
    remoteStream = emitStream @remoteEmitter
    remoteStream.pipe @model.writeStream

  afterEach ->
    @emitter.removeAllListeners()
    @remoteEmitter.removeAllListeners()

  describe 'acks', ->
    describe 'the first subscribe', ->
      beforeEach ->
        @model.readStream.resume()

      describe 'where target is a path', ->
        beforeEach ->
          called = false
          {id} = @model.subscribe 'collection.1', (err, @result) =>
            expect(err).to.equal null
            called = true
          expect(called).to.equal false
          @doc =
            id: 1
            name: 'Bryan'
            _v_: 0
          @remoteEmitter.emit 'ack.sub',
            id: id
            docs:
              'collection.1':
                snapshot: @doc
            pointers:
              'collection.1': true
          expect(called).to.equal true

        it 'should callback with a scoped model', ->
          expect(@result).to.be.a Model
          expect(@result.path()).to.equal('collection.1')

        it 'should initialize the proper documents and versions', ->
          expect(@result.get()).to.eql @doc
          expect(@model.version('collection.1')).to.equal 0

      it 'should re-send subscribes at intervals until receiving "ack.sub"', (done) ->
        cb = sinon.spy()
        @emitter.on 'sub', cb
        @model.subscribe 'collection.1'
        setTimeout ->
          expect(cb).to.have.callCount(2)
          done()
        , 600

    describe 'subsequent subscribes', ->
      beforeEach ->
        @model.readStream.resume()

      describe 'when subsequent result includes a later version of a prior doc', ->
        before ->
          @subAckOne =
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  _v_: 0
            pointers:
              'collection.1.name': true

        describe 'and there are field scope differences', ->
          describe 'and no incoming operations', ->
          describe 'and incoming operations', ->
            before -> @subAckTwo =
              docs:
                'collection.1':
                  snapshot:
                    id: 1
                    name: 'Brian'
                    height: 6
                    _v_: 3
                  # This would be ops since _v_ 0
                  ops: [
                    transaction.create(id: 'other-client.1', ver: 0, method: 'set', args: ['collection.1.x', 1])
                    transaction.create(id: 'other-client.2', ver: 1, method: 'set', args: ['collection.1.name', 'Brian'])
                    transaction.create(id: 'other-client.3', ver: 2, method: 'set', args: ['collection.1.y', 5])
                  ]
              pointers:
                'collection.1.height': true

            it 'should update to the new snapshots', ->
              {id: idOne} = @model.subscribe 'collection.1.name', (err, $name) =>
                expect(err).to.equal null
                expect($name.get()).to.equal 'Bryan'
                expect(@model.get('collection.1.height')).to.equal undefined

                {id: idTwo} = @model.subscribe 'collection.1.height', (err, $height) =>
                  expect(err).to.equal null
                  expect(@model.get('collection.1.height')).to.equal 6
                  expect($height.get()).to.equal 6
                  expect($name.get()).to.equal 'Brian'

                # Emulate the server getting back to the client with the second
                # subscribe's results.
                @subAckTwo.id = idTwo
                @remoteEmitter.emit 'ack.sub', @subAckTwo

              # Emulate the server getting back to the client with the first
              # subscribe's results.
              @subAckOne.id = idOne
              @remoteEmitter.emit 'ack.sub', @subAckOne

            it 'should transform pending txns against the received ops'
            it 'should apply the transformed pending ops to the newly set snapshot'
            it 'should transform inflight txns against the received ops if not one of the received ops'
            it 'should not apply the inlfight ops to the new snapshot if they are all part of the received ops'

        describe 'and there are no field scope differences', ->
          it 'should transform pending txns against the subscription ops after updating the snapshot'
          it 'should apply the incoming ops instead of updating the new snapshots'

    describe 'duplicate subscribes', ->
      beforeEach ->
        @model.readStream.resume()

      describe 'while connected', ->
        it 'should not send a message', ->
          cb = sinon.spy()
          @emitter.on 'sub', cb
          @model.subscribe 'collection.1.name'
          @model.subscribe 'collection.1.name'
          expect(cb).to.have.callCount(1)

    # Test re-connection subscription
    describe 'a subscription request that includes additional snapshot information', ->
      describe 'that responds before local transactions', ->
        it 'should apply the received operations'

      describe 'that responds after local transactions', ->
        it 'should apply the received operations after transforming them against the local ones'
