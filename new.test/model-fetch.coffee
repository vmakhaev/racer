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

  describe 'on a path', ->
    describe 'first fetch', ->
      beforeEach ->
        @doc = id: 1, name: 'Bryan', age: 27, _v_: 0

      describe 'minimal result latency', ->
        beforeEach (done) ->
          @model.fetch 'collection.1', (err, @result) =>
            expect(err).to.equal null
            done()
          @remoteEmitter.emit 'ack.fetch',
            docs:
              'collection.1':
                snapshot: @doc
            pointers:
              'collection.1': true

        it 'should callback with a scoped model', ->
          expect(@result).to.be.a Model
          expect(@result.path()).to.equal 'collection.1'

        it 'should initialize the proper documents and versions', ->
          expect(@result.get()).to.eql @doc
          expect(@model.version('collection.1')).to.equal 0

      describe 'non-trivial result latency', ->
        it 'should re-send fetches at intervals until receiving "ack.fetch"', (done) ->
          cb = sinon.spy()
          @emitter.on 'fetch', cb
          @model.fetch 'collection.1'
          setTimeout ->
            expect(cb).to.have.callCount(2)
            done()
          , 600

      describe 'compound fetches', ->
        describe 'who designate same doc, with different whitelist doc fields', ->
        describe 'where one fetch is a subset of another fetch', ->
        describe 'where all fetches designate mutually exclusive docs', ->

    describe 'subsequent fetches', ->
      describe 'whose doc is equivalent to the first fetch, with different whitelist doc fields', ->
        it 'should ask the server for both fetches again (to maintain version consistency)', ->
          {id} = @model.fetch 'collection.1.name'
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  _v_: 0
            pointers:
              'collection.1.name': true

          cb = sinon.spy()
          @emitter.on 'fetch', cb
          {id} = @model.fetch 'collection.1.age'
          expect(cb).to.be.calledWithEql [
            [
              id
              {
                t: 'collection.1.age'
                v: 0
                f: ['name'] # Other fields
              }
            ]
          ]

        it 'should update the result of both fetches, if both modified to a later version', (done) ->
          {id} = @model.fetch 'collection.1.name', (err, $name) =>
            expect($name.get()).to.equal 'Bryan'
            expect(@model.get('collection.1.age')).to.equal undefined
            {id} = @model.fetch 'collection.1.age', (err, $age) ->
              expect($name.get()).to.equal 'Brian'
              expect($age.get()).to.equal 28
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.1':
                  ops: [
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.name', 'Brian'], ver: 0
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.age', 28], ver: 1
                  ]
              pointers:
                'collection.1.age': true
          expect(@model.get('collection.1.name')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  _v_: 0
            pointers:
              'collection.1.name': true

      describe 'whose result is a subset of the first fetch', ->
        it 'should ask the server for the first fetch again (to maintain version consistency)', ->
          {id} = @model.fetch 'collection.1'
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  age: 27
                  _v_: 0
            pointers:
              'collection.1': true

          cb = sinon.spy()
          @emitter.on 'fetch', cb
          {id} = @model.fetch 'collection.1.age'
          expect(cb).to.be.calledWithEql [
            [
              id
              {
                t: 'collection.1.age'
                o: 'collection.1' # 'o' for override
                v: 0
              }
            ]
          ]

        it 'should provide a result for the subsequent fetch', (done) ->
          {id} = @model.fetch 'collection.1', (err, $doc) =>
            expect($doc.get()).to.eql
              id: 1
              name: 'Bryan'
              age: 27
            {id} = @model.fetch 'collection.1.age', (err, $age) =>
              expect($age.get()).to.equal 28
              expect(@model.get('collection.1.age')).to.equal 28
              expect($doc.version()).to.equal 2
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.1':
                  ops: [
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.name', 'Brian'], ver: 0
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.age', 28], ver: 1
                  ]
              pointers:
                'collection.1.age': true
          expect(@model.get('collection.1')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  age: 27
                  _v_: 0
            pointers:
              'collection.1': true

        it 'should update the result of the first fetch, if it was modified to a later version', (done) ->
          {id} = @model.fetch 'collection.1', (err, $doc) =>
            expect($doc.get()).to.eql
              id: 1
              name: 'Bryan'
              age: 27
            {id} = @model.fetch 'collection.1.age', (err, $age) =>
              expect($doc.get()).to.eql
                id: 1
                name: 'Brian'
                age: 28
              expect($doc.version()).to.equal 2
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.1':
                  ops: [
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.name', 'Brian'], ver: 0
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.age', 28], ver: 1
                  ]
              pointers:
                'collection.1.age': true
          expect(@model.get('collection.1')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Bryan'
                  age: 27
                  _v_: 0
            pointers:
              'collection.1': true

      describe 'whose docs are a superset of the first fetch', ->
        it 'should do only the recent fetch', ->
          {id} = @model.fetch 'collection.1.age'
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  age: 27
                  _v_: 0
            pointers:
              'collection.1.age': true

          cb = sinon.spy()
          @emitter.on 'fetch', cb
          {id} = @model.fetch 'collection.1'
          expect(cb).to.be.calledWithEql [
            [
              id
              {
                t: 'collection.1'
                v: 0
              }
            ]
          ]

        it 'should update the first fetch results based on the recent fetch results', (done) ->
          {id} = @model.fetch 'collection.1.age', (err, $age) =>
            expect(@model.get('collection.1.age')).to.equal 27
            expect($age.get()).to.equal 27
            {id} = @model.fetch 'collection.1', (err, $doc) =>
              expect($age.get()).to.equal 28
              expect(@model.get('collection.1.age')).to.equal 28
              expect($age.version()).to.equal 2
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.1':
                  snapshot:
                    id: 1
                    name: 'Brian'
                    age: 28
                    _v_: 2
                  ops: [
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.name', 'Brian'], ver: 0
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.age', 28], ver: 1
                  ]
              pointers:
                'collection.1': true
          expect(@model.get('collection.1.age')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  age: 27
                  _v_: 0
            pointers:
              'collection.1.age': true

        it 'should provide a result for the recent fetch', (done) ->
          {id} = @model.fetch 'collection.1.age', (err, $age) =>
            expect(@model.get('collection.1')).to.eql
              id: 1
              age: 27
            {id} = @model.fetch 'collection.1', (err, $doc) =>
              expectedDoc = id: 1, name: 'Brian', age: 28
              expect(@model.get('collection.1')).to.eql expectedDoc
              expect($doc.get()).to.eql expectedDoc
              expect($doc.version()).to.equal 2
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.1':
                  snapshot:
                    id: 1
                    name: 'Brian'
                    age: 28
                    _v_: 2
                  ops: [
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.name', 'Brian'], ver: 0
                    transaction.create id: 'other-client.1', method: 'set', args: ['collection.1.age', 28], ver: 1
                  ]
              pointers:
                'collection.1': true
          expect(@model.get('collection.1')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  age: 27
                  _v_: 0
            pointers:
              'collection.1.age': true

      describe 'whose doc is mutually exclusive from the first fetch', ->
        it 'should do only the recent fetch', ->
          {id} = @model.fetch 'collection.1'
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Brian'
                  _v_: 0
            pointers:
              'collection.1': true

          cb = sinon.spy()
          @emitter.on 'fetch', cb
          {id} = @model.fetch 'collection.2'
          expect(cb).to.be.calledWithEql [
            [
              id
              {t: 'collection.2'}
            ]
          ]

        it 'should not update the first fetch results', (done) ->
          {id} = @model.fetch 'collection.1', (err, $docOne) =>
            expectedOne =
              id: 1
              name: 'Brian'
            expect(@model.get('collection.1')).to.eql expectedOne
            expect($docOne.get()).to.eql expectedOne
            {id} = @model.fetch 'collection.2', (err, $docTwo) =>
              expect(@model.get('collection.1')).to.eql expectedOne
              expect($docOne.get()).to.eql expectedOne
              expect($docOne.version()).to.equal 0
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.2':
                  snapshot:
                    id: 2
                    name: 'Nate'
                    _v_: 1
              pointers:
                'collection.2': true
          expect(@model.get('collection.1')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Brian'
                  _v_: 0
            pointers:
              'collection.1': true

        it 'should provide a result for the recent fetch', (done) ->
          {id} = @model.fetch 'collection.1', (err, $docOne) =>
            expect(@model.get('collection.2')).to.equal undefined
            {id} = @model.fetch 'collection.2', (err, $docTwo) =>
              expectedTwo =
                id: 2
                name: 'Nate'
              expect(@model.get('collection.2')).to.eql expectedTwo
              expect($docTwo.get()).to.eql expectedTwo
              expect($docTwo.version()).to.equal 1
              done()
            @remoteEmitter.emit 'ack.fetch',
              id: id
              docs:
                'collection.2':
                  snapshot:
                    id: 2
                    name: 'Nate'
                    _v_: 1
              pointers:
                'collection.2': true
          expect(@model.get('collection.2')).to.equal undefined
          @remoteEmitter.emit 'ack.fetch',
            id: id
            docs:
              'collection.1':
                snapshot:
                  id: 1
                  name: 'Brian'
                  _v_: 0
            pointers:
              'collection.1': true

  describe 'acks', ->
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
              # nextTick in order to have access to idTwo via fetch returning
              process.nextTick ->
                cb()
                expect(msg).to.eql [
                  idTwo
                  {
                    t: 'collection.1'
                    v: 0
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
