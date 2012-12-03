emitStream = require 'emit-stream'
sinon = require 'sinon'
{BrowserModel: Model} = require '../test/util/model'
expect = require 'expect.js'

describe 'Model read stream', ->
  it 'should stream transactions', ->
    model = new Model
    emitter = emitStream model.readStream
    callback = sinon.spy()
    emitter.on 'txn', callback
    model.set 'collection.1', a: 1, b: 2
    expect(callback).to.be.called
