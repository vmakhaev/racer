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
  beforeEach (done) ->
    mongo.dropDatabase =>
      redis.flushdb =>
        @model = store.createModel()
        done()

  after ->
    mongo.close()
    redis.quit()

  describe 'getters', ->

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

    it 'should not include the calculated output when getting an ancestor node', ->
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

    it 'should be able to get a nested part of a virtual', ->
      @model.virtual 'users', 'virt',
        inputs: ['a', 'b']
        get: (a, b) ->
          return {
            x:
              z: a
            y:
              z: b
          }
      userId = @model.add 'users', a: 'A', b: 'B'
      expect(@model.get "users.#{userId}.virt.x.z").to.equal('A')
      expect(@model.get "users.#{userId}.virt.y.z").to.equal('B')

    describe 'for virtuals that output to array values', ->
      it 'should calculate the correct output', ->
        @model.virtual 'groups', 'initials',
          inputs: ['names']
          get: (names) ->
            (first.charAt(0) + ' ' + last.charAt(0) for {first, last} in names)
        groupId = @model.add 'groups',
          names: [
            {first: 'Tony', last: 'Stark'}
          ]
        expect(@model.get "groups.#{groupId}.initials").to.eql [
          'T S'
        ]

      it 'should be able to get a nested part of a virtual', ->
        @model.virtual 'widgets', 'virt',
          inputs: ['input']
          get: (input) ->
            ({abc: x} for x in input)
        widgetId = @model.add 'widgets', input: ['X', 'Y']
        expect(@model.get "widgets.#{widgetId}.virt").to.eql [{abc: 'X'}, {abc: 'Y'}]
        expect(@model.get "widgets.#{widgetId}.virt.0.abc").to.equal('X')

  describe 'persistence', ->
    it 'should not persist', (done) ->
      declareVirtual = (model) ->
        model.virtual 'users', 'name.full',
          inputs: ['name.first', 'name.last']
          get: (firstName, lastName) ->
            return firstName + ' ' + lastName

      declareVirtual @model
      userId = @model.add 'users',
        name:
          first: 'Tony'
          last: 'Stark'
      , (err) ->
        model = store.createModel()
        declareVirtual model
        $user = model.at "users.#{userId}"
        $user.fetch (err) ->
          expect($user.get()).to.eql
            id: userId
            name:
              first: 'Tony'
              last: 'Stark'
          done()

  describe 'setter', ->
    it 'should write back to inputs'

  describe 'events', ->
    it 'should emit events on output when input changes', (done) ->
      @model.virtual 'users', 'name.full',
        inputs: ['name.first', 'name.last']
        get: (firstName, lastName) ->
          return firstName + ' ' + lastName
      userId = @model.add 'users',
        name:
          first: 'Tony'
          last: 'Stark'
      @model.on 'change', 'users.*.name.full', (userId, newName, oldName) ->
        expect(newName).to.equal 'Ned Stark'
        expect(oldName).to.equal 'Tony Stark'
        done()
      @model.set "users.#{userId}.name.first", 'Ned'

    it 'should emit events on input when output changes'

    it 'should emit granular change event under output when input changes', (done) ->
      @model.virtual 'users', 'virt',
        inputs: ['a', 'b']
        get: (a, b) ->
          return {
            x:
              z: a
            y:
              z: b
          }
      userId = @model.add 'users', a: 'A', b: 'B'
      @model.on 'change', 'users.*.virt.x.z', (userId, newVal, oldVal) ->
        expect(newVal).to.equal('Z')
        expect(oldVal).to.equal('A')
        done()
      @model.set "users.#{userId}.a", 'Z'

    describe 'for virtuals that output to array values', ->
      it 'should emit events on output when input changes', (done) ->
        @model.virtual 'groups', 'initials',
          inputs: ['names']
          get: (names) ->
            (first.charAt(0) + ' ' + last.charAt(0) for {first, last} in names)
        groupId = @model.add 'groups',
          names: [
            {first: 'Tony', last: 'Stark'}
          ]
        expect(@model.get "groups.#{groupId}.initials").to.eql [
          'T S'
        ]
        @model.on 'insert', 'groups.*.initials', (groupId, index, inserted) ->
          expect(inserted).to.have.length(1)
          expect(inserted[0]).to.equal 'N S'
          done()
        @model.push "groups.#{groupId}.names", {first: 'Ned', last: 'Stark'}
