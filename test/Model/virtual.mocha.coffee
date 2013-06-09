expect = require 'expect.js'
racer = require '../../lib/index'
{LiveDbMongo} = require 'livedb-mongo'
mongoskin = require 'mongoskin'
mongo = mongoskin.db('mongodb://localhost:27017/test?auto_reconnect', safe: true)
redis = require('redis').createClient()
redis.select 8

store = racer.createStore
  db: new LiveDbMongo(mongo)
  redis: redis

describe 'virtual', ->
  beforeEach ->
    mongo.dropDatabase()
    redis.flushdb()
    @model = store.createModel()

  after ->
    mongo.close()
    redis.quit()

  it 'should calculate the correct output', ->
    @model.virtual 'users', 'name.full',
      inputs: ['name.first', 'name.last']
      get: (firstName, lastName) ->
        return firstName + ' ' + lastName

    userId = @model.add 'users',
      name:
        first: 'Tony'
        last: 'Stark'
    expect(@model.get "users.#{userId}.name.full").to.equal 'Tony Stark'

  it.only 'should include the calculated output when getting an ancestor node', ->
    @model.virtual 'users', 'name.full',
      inputs: ['name.first', 'name.last']
      get: (firstName, lastName) ->
        return firstName + ' ' + lastName

    userId = @model.add 'users',
      name:
        first: 'Tony'
        last: 'Stark'
    expect(@model.get "users.#{userId}.name").to.eql
      first: 'Tony'
      last: 'Stark'
      full: 'Tony Stark'


  it 'should be able to get a nested part of a virtual'

  it 'should not persist'

  describe 'setter', ->
    it 'should write back to inputs'
#      @model.virtual 'widgets', 'name.full', 'name.first', 'name.last',
#        get: (firstName, lastName) ->
#          return firstName + lastName
#        set: (fullName) ->
#          [firstName, lastName] = fullName.split ' '
#          @set 'firstName', firstName
#          @set 'lastName', lastName

  describe 'events', ->
    it 'should emit events on output when input changes'

    it 'should emit events on input when output changes'

    it 'should emit granular change event under output when input changes'
