emitStream = require 'emit-stream'
sinon = require 'sinon'
{EventEmitter} = require 'events'
{BrowserModel: Model} = require '../test/util/model'
transaction = require('../lib/transaction')
expect = require 'expect.js'

describe 'Model unfetch', ->
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
    beforeEach (done) ->
      {id} = @model.fetch 'collection.1', (err, @result) =>
        done()
      @remoteEmitter.emit 'ack.fetch',
        id: id
        docs:
          'collection.1':
            snapshot:
              id: 1
              name: 'Bryan'
              _v_: 0
        pointers:
          'collection.1': true

    it 'should clean up data that belongs to no other fetches or subscribes', ->
      expect(@model.get('collection.1')).to.not.equal undefined
      @model.unfetch 'collection.1'
      expect(@model.get('collection.1')).to.equal undefined

    it 'should not clean up data that belongs to a duplicate fetch', ->
      @model.fetch 'collection.1', (err, result) =>
        expect(@model.get('collection.1')).to.not.equal undefined
        @model.unfetch 'collection.1'
        expect(@model.get('collection.1')).to.not.equal undefined
        @model.unfetch 'collection.1'
        expect(@model.get('collection.1')).to.equal undefined

    it 'should not clean up data that overlaps with a non-duplicate fetch', ->
      @model.fetch 'collection.1.name'
      expect(@model.get('collection.1')).to.not.equal undefined
      @model.unfetch 'collection.1'
      expect(@model.get('collection.1')).to.not.equal undefined

    it 'should clean up data that overlaps with other fetches, if all relevant fetches are unfetched', ->
      @model.fetch 'collection.1.name'
      expect(@model.get('collection.1')).to.not.equal undefined
      @model.unfetch 'collection.1'
      @model.unfetch 'collection.1.name'
      expect(@model.get('collection.1')).to.equal undefined
