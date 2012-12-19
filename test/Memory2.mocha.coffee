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
      collection = memory.get 'colors'
      expect(collection).equal undefined

    it 'can get a defined collection', ->
      memory = new Memory
      memory.world =
        colors:
          green: {}
          red: {}
      collection = memory.get 'colors'
      expect(collection).eql green: {}, red: {}

    it 'can get a document on an undefined collection', ->
      memory = new Memory
      document = memory.get 'colors', 'green'
      expect(document).equal undefined

    it 'can get an undefined document on an defined collection', ->
      memory = new Memory
      memory.world =
        colors: {}
      document = memory.get 'colors', 'green'
      expect(document).equal undefined

    it 'can get a defined document', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
      document = memory.get 'colors', 'green'
      expect(document).eql id: 'green'

    it 'can get a property on an undefined document', ->
      memory = new Memory
      memory.world =
        colors: {}
      property = memory.get 'colors', 'green', 'id'
      expect(property).equal undefined

    it 'can get an undefined property on a defined document', ->
      memory = new Memory
      memory.world =
        colors:
          green: {}
      property = memory.get 'colors', 'green', 'id'
      expect(property).equal undefined

    it 'can get a defined property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
      property = memory.get 'colors', 'green', 'id'
      expect(property).equal 'green'

    it 'can get a falsey property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            id: 'green'
            value: false
      property = memory.get 'colors', 'green', 'value'
      expect(property).equal false

    it 'can get a nested property on an undefined property', ->
      memory = new Memory
      memory.world =
        colors:
          green: {}
      value = memory.get 'colors', 'green', 'rgb', 'red'
      expect(value).equal undefined

    it 'can get an undefined nested property on a defined property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: {}
      value = memory.get 'colors', 'green', 'rgb', 'red'
      expect(value).equal undefined

    it 'can get a defined nested property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb:
              red: 0
              green: 255
              blue: 0
      value = memory.get 'colors', 'green', 'rgb', 'red'
      expect(value).equal 0

    it 'can get a multi-nested property on an undefined nested property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: {}
      value = memory.get 'colors', 'green', 'rgb', 'green.float'
      expect(value).equal undefined

    it 'can get an undefined multi-nested property on a defined nested property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: {}
      value = memory.get 'colors', 'green', 'rgb', 'green.float'
      expect(value).equal undefined

    it 'can get a defined multi-nested property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb:
              red:
                float: 0
              green:
                float: 1
              blue:
                float: 0
      value = memory.get 'colors', 'green', 'rgb', 'green.float'
      expect(value).equal 1

    it 'can get a method property', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            empty: ''
      value = memory.get 'colors', 'green', 'empty', 'charAt'
      expect(value).equal ''.charAt

    it 'can get an array member', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: [0, 255, 0]
      value = memory.get 'colors', 'green', 'rgb', '1'
      expect(value).equal 255

    it 'can get array length', ->
      memory = new Memory
      memory.world =
        colors:
          green:
            rgb: [0, 255, 0]
      value = memory.get 'colors', 'green', 'rgb', 'length'
      expect(value).equal 3

  describe 'set', ->

