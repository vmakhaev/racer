{expect} = require './util'
Memory = require '../lib/Memory2'

describe 'Memory', ->

  describe 'get', ->

    it 'can get all data', ->
      memory = new Memory
      world = memory.get()
      expect(world).eql {}

    it 'can get an undefined collection', ->
      memory = new Memory
      collection = memory.get ['colors']
      expect(collection).equal undefined

    it 'can get a defined collection', ->
      memory = new Memory
      memory.world =
        colors:
          green: {}
          red: {}
      collection = memory.get ['colors']
      expect(collection).eql green: {}, red: {}

    it 'can get a document on an undefined collection', ->
      memory = new Memory
      document = memory.get ['colors', 'green']
      expect(document).equal undefined

    it 'can get an undefined document on an defined collection', ->
      memory = new Memory
      memory.world =
        colors: {}
      document = memory.get ['colors', 'green']
      expect(document).equal undefined

    it 'can get a defined document', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
      document = memory.get ['colors', 'green']
      expect(document).eql id: 'green'

    it 'can get a property on an undefined document', ->
      memory = new Memory
      memory.world =
        colors: {}
      property = memory.get ['colors', 'green', 'id']
      expect(property).equal undefined

    it 'can get an undefined property on a defined document', ->
      memory = new Memory
      memory.world =
        colors:
          green: {}
      property = memory.get ['colors', 'green', 'id']
      expect(property).equal undefined

    it 'can get a defined property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
      property = memory.get ['colors', 'green', 'id']
      expect(property).equal 'green'

    it 'can get a false property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
            shown: false
      property = memory.get ['colors', 'green', 'shown']
      expect(property).equal false

    it 'can get a null property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
            shown: null
      property = memory.get ['colors', 'green', 'shown']
      expect(property).equal null

    it 'can get a method property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            empty: ''
      value = memory.get ['colors', 'green', 'empty', 'charAt']
      expect(value).equal ''.charAt

    it 'can get an array member', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: [0, 255, 0]
      value = memory.get ['colors', 'green', 'rgb', '1']
      expect(value).equal 255

    it 'can get an array length', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: [0, 255, 0]
      value = memory.get ['colors', 'green', 'rgb', 'length']
      expect(value).equal 3

  describe 'set', ->

    it 'can set an empty doc', ->
      memory = new Memory
      previous = memory.set ['colors', 'green'], {}
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green: {}

    it 'can set a property', ->
      memory = new Memory
      previous = memory.set ['colors', 'green', 'shown'], false
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green:
            shown: false

    it 'can set multi-nested property', ->
      memory = new Memory
      previous = memory.set ['colors', 'green', 'rgb', 'green', 'float'], 1
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green:
            rgb:
              green:
                float: 1

    it 'can set on an existing document', ->
      memory = new Memory
      previous = memory.set ['colors', 'green'], {}
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green: {}
      previous = memory.set ['colors', 'green', 'shown'], false
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green:
            shown: false

    it 'returns the previous value on set', ->
      memory = new Memory
      previous = memory.set ['colors', 'green', 'shown'], false
      expect(previous).equal undefined
      expect(memory.get()).eql
        colors:
          green:
            shown: false
      previous = memory.set ['colors', 'green', 'shown'], true
      expect(previous).equal false
      expect(memory.get()).eql
        colors:
          green:
            shown: true

    it 'creates an implied array on set', ->
      memory = new Memory
      memory.set ['colors', 'green', 'rgb', '2'], 0
      memory.set ['colors', 'green', 'rgb', '1'], 255
      memory.set ['colors', 'green', 'rgb', '0'], 0
      expect(memory.get()).eql
        colors:
          green:
            rgb: [0, 255, 0]

    it 'throws an error when setting without a collection', ->
      memory = new Memory
      expect(-> memory.set [], 'x').throwError()

    it 'throws an error when setting without a document', ->
      memory = new Memory
      expect(-> memory.set ['colors'], 'x').throwError()

  describe 'del', ->

    it 'can del on an undefined document', ->
      memory = new Memory
      previous = memory.del ['colors', 'green']
      expect(previous).equal undefined
      expect(memory.get()).eql {}

    it 'can del on a document', ->
      memory = new Memory
      memory.set ['colors', 'green'], {}
      previous = memory.del ['colors', 'green']
      expect(previous).eql {}
      expect(memory.get()).eql colors: {}

    it 'can del on a nested property', ->
      memory = new Memory
      memory.set ['colors', 'green', 'rgb'], [
        {float: 0, int: 0}
        {float: 1, int: 255}
        {float: 0, int: 0}
      ]
      previous = memory.del ['colors', 'green', 'rgb', '0', 'float']
      expect(previous).eql 0
      expect(memory.get ['colors', 'green', 'rgb']).eql [
        {int: 0}
        {float: 1, int: 255}
        {float: 0, int: 0}
      ]

    it 'throws an error when deleting without a collection', ->
      memory = new Memory
      expect(-> memory.del []).throwError()

    it 'throws an error when deleting without a document', ->
      memory = new Memory
      expect(-> memory.del ['colors']).throwError()

  describe 'push', ->

    it 'can push on an undefined property', ->
      memory = new Memory
      len = memory.push ['users', 'chris', 'friends'], ['jim', 'dan']
      expect(len).equal 2
      expect(memory.get()).eql
        users:
          chris:
            friends: ['jim', 'dan']

    it 'can push on a defined arry', ->
      memory = new Memory
      len = memory.push ['users', 'chris', 'friends'], ['jim', 'dan']
      expect(len).equal 2
      len = memory.push ['users', 'chris', 'friends'], ['sue']
      expect(len).equal 3
      expect(memory.get()).eql
        users:
          chris:
            friends: ['jim', 'dan', 'sue']

    it 'throws an error when pushing without a collection', ->
      memory = new Memory
      expect(-> memory.push [], ['x']).throwError()

    it 'throws an error when pushing without a document', ->
      memory = new Memory
      expect(-> memory.push ['users'], ['x']).throwError()

    it 'throws an error when pushing without a property', ->
      memory = new Memory
      expect(-> memory.push ['users', 'chris'], ['x']).throwError()

    it 'throws a TypeError when pushing on a non-array', ->
      memory = new Memory
      memory.set ['users', 'chris', 'friends'], {}
      expect(-> memory.push ['users', 'chris', 'friends'], ['x']).throwError (err) ->
        expect(err).a TypeError
