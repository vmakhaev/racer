Promise = require '../src/Promise'
should = require 'should'
{wrapTest} = require './util'

module.exports =
  'should execute immediately if the Promise is already fulfilled': wrapTest (done) ->
    p = new Promise
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      done()

  '''should execute immediately using the appropriate scope
  if the Promise is already fulfilled''': wrapTest (done) ->
    p = new Promise
    p.fulfill true
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'

  'should wait to execute a callback until the Promise is fulfilled': wrapTest (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      done()
    p.fulfill true

  '''should wait to execute a callback using the appropriate scope
  until the Promise is fulfilled''': wrapTest (done) ->
    p = new Promise
    p.callback (val) ->
      val.should.be.true
      @foo.should.equal 'bar'
      done()
    , foo: 'bar'
    p.fulfill true

  '''should wait to execute multiple callbacks until the Promise is
    fulfilled''': wrapTest (done) ->
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
  , 2

  '''should execute multiple callbacks immediately if the
  Promise is already fulfilled''': wrapTest (done) ->
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
  , 2

  '''should execute a callback decalared before fulfillment
  and then declare a subsequent callback immediately
  after fulfillment''': wrapTest (done) ->
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
  , 2

  '''clearValue should clear the fulfilled value of a Promise
  and invoke only new callbacks upon a subsequent fulfillment''': wrapTest (done) ->
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
  , 2

  '''Promise.parallel should create a new promise that is not fulfilled
  until all of the component Promises are fulfilled''': wrapTest (done) ->
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
      val.should.be.true
      p1Val.should.equal 'hello'
      p2Val.should.equal 'world'
      done()
    p1.fulfill 'hello'
    p2.fulfill 'world'

  '''a promise resulting from Promise.parallel should clear its value
  if at least one of its component Promises clears its values''': wrapTest (done) ->
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
  , 2

  '''Promise.transform should create a new promise that applies the transform
  function to the fulfilled value and then passes the tranformed value to the
  promise callback''': wrapTest (done) ->
    transP = Promise.transform (val) -> val * 100
    transP.callback (val) ->
      val.should.equal 100
      done()
    transP.fulfill 1

  '''Promise.pipe(promiseA, promiseB) should create a new promise that passes the 
  result of promiseA to promiseB and whose fulfilled value is the list of values of
  promiseA.fulfill and promiseB.fulfill''': wrapTest (done) ->
    promiseA = new Promise
    promiseB = Promise.transform (val) -> val * 2
    promise = Promise.pipe promiseA, promiseB
    promise.callback ([valA, valB]) ->
      valA.should.equal 10
      valB.should.equal 20
      done()
    promiseA.callback (val) ->
      val.should.equal 10
    promiseB.callback (val) ->
      val.should.equal 20
    promise.fulfill 10
