emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model fetch', ->
  beforeEach ->
    @model = new Model _clientId: 'x'
    @model.readStream.resume()
    @emitter = @model.broadcaster
    @remoteEmitter = new EventEmitter
    remoteStream = emitStream @remoteEmitter
    remoteStream.pipe @model.writeStream

  afterEach ->
    @emitter.removeAllListeners()
    @remoteEmitter.removeAllListeners()

  describe 'acks', ->
    describe 'the first fetch', ->
      describe 'where target is a path', ->
        beforeEach ->
          called = false
          @model.fetch 'collection.1', (err, @result) =>
            expect(err).to.equal null
            called = true
          expect(called).to.equal false
          @doc =
            id: 1
            name: 'Bryan'
            _v_: 0
          @remoteEmitter.emit 'ack.fetch',
            docs:
              'collection.1':
                snapshot: @doc
            pointers:
              'collection.1': true
          expect(called).to.equal true

        it 'should callback with a scoped model', ->
          expect(@result).to.be.a Model
          expect(@result.path()).to.equal 'collection.1'

        it 'should initialize the proper documents and versions', ->
          expect(@result.get()).to.eql @doc
          expect(@model.version('collection.1')).to.equal 0

      it 'should re-send fetches at intervals until receiving "ack.fetch"', (done) ->
        cb = sinon.spy()
        @emitter.on 'fetch', cb
        @model.fetch 'collection.1'
        setTimeout ->
          expect(cb).to.have.callCount(2)
          done()
        , 600

    describe 'subsequent fetches', ->
      describe 'without an overlapping subscription', ->
        describe 'when subsequent result includes a later version of a prior doc', ->
          describe 'and there are no field scope differences', ->
            it 'should transform (against local ops) and apply incoming ops'
            it 'should apply incoming ops without xf (if no local ops)', ->
              @model.fetch 'collection.1', (err, $docA) =>
                expect(err).to.equal null
                expect($docA.get()).to.eql {id: 1, name: 'Bryan'}
                expect($docA.version()).to.equal 0
                @model.fetch 'collection.1', (err, $docB) =>
                  expect(err).to.equal null
                  expect($docB.get()).to.eql {id: 1, name: 'Brian'}
                  expect($docB.version()).to.equal 1
                @remoteEmitter.emit 'ack.fetch',
                  docs:
                    'collection.1':
                      ops: [
                        transaction.create id: 'other-client.1', ver: 0, method: 'set', args: ['collection.1.name', 'Brian']
                      ]
                  pointers:
                    'collection.1': true
              @remoteEmitter.emit 'ack.fetch',
                docs:
                  'collection.1':
                    snapshot:
                      id: 1
                      name: 'Bryan'
                      _v_: 0
                pointers:
                  'collection.1': true

          describe 'and there are field scope differences', ->
            it "should serially (1) xf local ops against remote ops, (2) apply the xf'ed local ops to incoming snapshot, (3) set to result of (2)"

    describe 'duplicate fetches', ->
      describe 'without an overlapping subscription', ->
        it 'should include document version data in its outgoing message', (done) ->
          cb = sinon.spy()
          {id: idOne} = @model.fetch 'collection.1', (err, $docA) =>
            @emitter.on 'fetch', (msg) ->
              console.log msg
              # nextTick in order to have access to idTwo via fetch returning
              process.nextTick ->
                cb()
                expect(msg).to.eql [
                  idTwo
                  {
                    t: 'collection.1'
                    m:
                      ns: 'collection'
                      v:
                        1: 0
                  }
                ]
                expect(cb).to.be.calledOnce()
                done()
            {id: idTwo} = @model.fetch 'collection.1'
          @remoteEmitter.emit 'ack.fetch',
            id: idOne
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  _v_: 0
            pointers:
              'collection.1': true

      describe 'with an overlapping subscription', ->
