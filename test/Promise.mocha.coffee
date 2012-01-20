Promise = require '../src/Promise'
should = require 'should'
{calls} = require './util'

describe 'Promise', ->

  it 'should execute immediately if the Promise is already fulfilled', (done) ->
    p = new Promise
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      done()

  it 'should execute immediately using the appropriate scope if the Promise is already fulfilled', (done) ->
    p = new Promise
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'

  it 'should wait to execute a callback until the Promise is fulfilled', (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      done()
    p.fulfill true

  it 'should wait to execute a callback using the appropriate scope until the Promise is fulfilled', (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'
    p.fulfill true

  it 'should wait to execute multiple callbacks until the Promise is fulfilled', calls 2, (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'

    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'fighters'
      done()
    , foo: 'fighters'
    
    p.fulfill true

  it 'should execute multiple callbacks immediately if the Promise is already fulfilled', calls 2, (done) ->
    p = new Promise
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'

    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'fighters'
      done()
    , foo: 'fighters'

  it 'should execute a callback decalared before fulfillment and then declare a subsequent callback immediately after fulfillment', calls 2, (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'fighters'
      done()
    , foo: 'fighters'

  it 'clearValue should clear the fulfilled value of a Promise and invoke only new callbacks upon a subsequent fulfillment', calls 2, (done) ->
    p = new Promise
    counter = 0
    p.callback (val) ->
      val.should.equal 'first'
      (++counter).should.equal 1
      done()
    p.fulfill 'first'
    p.clearValue()
    p.callback (val) ->
      val.should.equal 'second'
      (++counter).should.equal 2
      done()
    p.fulfill 'second'

  it 'Promise.parallel should create a new promise that is not fulfilled until all of the component Promises are fulfilled', (done) ->
    p1 = new Promise
    p2 = new Promise
    p1Val = null
    p2Val = null
    p1.callback (val) ->
      p1Val = val
    p2.callback (val) ->
      p2Val = val
    p = Promise.parallel [p1, p2]
    p.callback (val) ->
      val.should.eql ['hello']
      p1Val.should.equal 'hello'
      p2Val.should.equal 'world'
      done()
    p1.fulfill 'hello'
    p2.fulfill 'world'

  'Promise.parallel should callback with an object if the input is a dictionary of promises': (done) ->
    p1 = new Promise
    p2 = new Promise
    p1Val = null
    p2Val = null
    p1.callback (val) ->
      p1Val = val
    p2.callback (val) ->
      p2Val = val
    p = Promise.parallel a: p1, b: p2
    p.callback (val) ->
      val.should.eql a: 'hello', b: 'world'
      p1Val.should.equal 'hello'
      p2Val.should.equal 'world'
      done()
    p1.fulfill 'hello'
    p2.fulfill 'world'

  it 'a promise resulting from Promise.parallel should clear its value if at least one of its component Promises clears its values', calls 2, (done) ->
    p1 = new Promise
    p2 = new Promise
    p = Promise.parallel [p1, p2]
    counter = 0
    p.callback (val) ->
      val.should.equal 'first'
      (++counter).should.equal 1
      done()

    p.fulfill 'first'

    p1.clearValue()

    p.callback (val) ->
      val.should.equal 'second'
      (++counter).should.equal 2
      done()

    p.fulfill 'second'

  '''Promise.transform should create a new promise that applies the transform
  function to the fulfilled value and then passes the tranformed value to the
  promise callback''': (done) ->
    transP = Promise.transform (val) -> val * 100
    transP.callback (val) ->
      val.should.equal 100
      done()
    transP.fulfill 1
