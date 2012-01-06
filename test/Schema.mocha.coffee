should = require 'should'
Schema = require '../src/Schema/Logical/Schema'

describe 'Schema', ->

  describe 'Schema.extend', ->
    it 'should return a constructor that inherits from Schema', ->
      User = Schema.extend 'User', 'users',
        name: String
      user = new User
      user.should.be.an.instanceof User
      user.should.be.an.instanceof Schema

    it 'should return a Schmea subclass that can extend itself', ->
      User = Schema.extend 'User', 'users',
        name: String

      Admin = User.extend 'Admin', 'admins',
        privileges: [String]

      admin = new Admin
      admin.should.be.an.instanceof Admin
      admin.should.be.an.instanceof User
      admin.should.be.an.instanceof Schema

  describe 'static methods', ->
    it 'a Schema subclass should inherit static methods', ->
      User = Schema.extend 'User', 'users',
        name: String

      counter = 0
      User.static 'newStatic', ->
        counter++
      Admin = User.extend 'Admin', 'admins',
        privileges: [String]

      Admin.newStatic()
      counter.should.equal 1

    it 'adding a static to a Schema should add that static to all descendant Schemas', ->
      User = Schema.extend 'User', 'users',
        name: String

      counter = 0

      Admin = User.extend 'Admin', 'admins',
        privileges: [String]

      SuperAdmin = Admin.extend 'SuperAdmin', 'super_admins',
        rootPassword: String

      User.static 'newStatic', ->
        counter++

      Admin.newStatic()
      SuperAdmin.newStatic()
      counter.should.equal 2

    it 'adding a static to a Schema should not add that method to ancestor Schemas', ->
      User = Schema.extend 'User', 'users',
        name: String

      counter = 0

      Admin = User.extend 'Admin', 'admins',
        privileges: [String]

      Admin.static 'newStatic', true

      Admin.newStatic.should.not.be.undefined
      should.equal undefined, User.newStatic
      should.equal undefined, Schema.newStatic

    it 'changing a parent static after assigning a child static of the same name should not over-write the child static', ->
      User = Schema.extend 'User', 'users',
        name: String

      counter = 0

      Admin = User.extend 'Admin', 'admins',
        privileges: [String]

      Admin.static 'newStatic', 'super'

      User.static 'newStatic', 'ballin'

      User.newStatic.should.equal 'ballin'
      Admin.newStatic.should.equal 'super'

  describe 'Schema.fromPath', ->
    it 'should return the schema and path remainder from an absolute path', ->
      User = Schema.extend 'User', 'users',
        name: String

      {Skema, path, id} = Schema.fromPath 'users.1.name'
      Skema.should.equal User
      id.should.equal '1'
      path.should.equal 'name'

  it 'an instantiated Schema doc initialized with attributes should be able to retrieve those attributes via get', ->
    User = Schema.extend 'User', 'users',
      name: String
    user = new User name: 'Brian'
    user.get('name').should.equal 'Brian'

  describe 'Casting', ->
    it 'should cast String attributes', ->
      User = Schema.extend 'User', 'users',
        _id: String
      s = new User _id: 1
      s.get('_id').should.equal '1'

    it 'should cast Number attributes', ->
      User = Schema.extend 'User', 'users',
        age: Number
      s = new User age: '26'
      s.get('age').should.equal 26

    it 'should cast [String] attributes', ->
      User = Schema.extend 'User', 'users',
        tags: [String]
      u = new User tags: [1, 2, 3, 4]
      u.get('tags').should.eql ['1', '2', '3', '4']

    it 'should cast [Number] attributes', ->
      User = Schema.extend 'User', 'users',
        luckyNumbers: [Number]
      s = new User luckyNumbers: ['4', 8, '12', 16]
      s.get('luckyNumbers').should.eql [4, 8, 12, 16]

    describe 'an attribute that is a CustomSchema document', ->
      Dog = Schema.extend 'Dog', 'dogs',
        _id: String
        name: String
      User = Schema.extend 'User', 'users',
        name: String
        pet: Dog

      it 'should cast CustomSchema attributes', ->
        s = new User pet: { _id: 5, name: 'Spot' }
        pet = s.get('pet')
        pet.should.be.an.instanceof Dog
        pet.get('_id').should.equal '5'
        pet.get('name').should.equal 'Spot'

      it 'should assign an instance of that schema when set to an object literal', ->
        u = new User name: 'Brian'
        u.set 'pet', name: 'Banana'
        u.get('pet').should.be.an.instanceof Dog
        u.get('pet').get('name').should.equal 'Banana'

      it 'should assign an instance of that schema when set to a schema instance', ->
        u = new User name: 'Brian'
        u.set 'pet', new Dog name: 'Banana'
        u.get('pet').should.be.an.instanceof Dog
        u.get('pet').get('name').should.equal 'Banana'

      it 'should raise an error if set to a non-matching Schema instance', ->
        u = new User name: 'Brian'
        didErr = false
        errMsg = null
        try
          u.set 'pet', new User name: 'Brian'
        catch e
          didErr = true
          errMsg = e.message
        didErr.should.be.true
        errMsg.indexOf('is neither an Object nor a Dog').should.not.equal -1

    describe 'an attribute that is an array of CustomSchema documents', ->
      Dog = Schema.extend 'Dog', 'dogs',
        _id: String
        name: String
        age: Number
      User = Schema.extend 'User', 'users',
        name: String
        pets: [Dog]

      it 'should cast [CustomSchema] attributes', ->
        u = new User pets: [
          { _id: 1, name: 'Banana', age: '2'}
          {_id: 2, name: 'Squeak', age: '8'}
        ]
        pets = u.get 'pets'
        pets.length.should.equal 2
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[0].get('age').should.equal 2
        pets[1].get('name').should.equal 'Squeak'
        pets[1].get('age').should.equal 8

      it 'should assign an array of Schema instance to the attribute of the same name, when set to an array of object literals',  ->
        u = new User name: 'Brian'
        u.set 'pets', [{name: 'Banana'}, {name: 'Squeak'}]
        pets = u.get('pets')
        pets.should.have.length 2
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'

      it 'should assign an array of Schema instances directly when set to this array', ->
        u = new User name: 'Brian'
        u.set 'pets', [
          new Dog name: 'Banana'
          new Dog name: 'Squeak'
        ]
        pets = u.get('pets')
        pets.should.have.length 2
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'

      it 'should raise an error if set to an array of SomeSchema instances that contains at least one non-matching SchemaB', ->
        u = new User name: 'Brian'
        didErr = false
        errMsg = null
        try
          u.set 'pets', [
            new User name: 'Banana'
            new Dog name: 'Squeak'
          ]
        catch e
          didErr = true
          errMsg = e.message
        didErr.should.be.true
        errMsg.indexOf('is neither an Object nor a Dog').should.not.equal -1

      it 'pushing an object literal onto the field should convert the literal into a Schema doc and append it to the attribute', ->
        u = new User name: 'Brian'
        u.set 'pets', [{name: 'Banana'}, {name: 'Squeak'}]
        u.push 'pets', {name: 'Pogo'}
        pets = u.get('pets')
        pets.should.have.length 3
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'
        pets[2].get('name').should.equal 'Pogo'

      it 'pushing a Schema instance onto the field should push this instance directly onto the existing array of instances', ->
        u = new User name: 'Brian'
        u.set 'pets', [{name: 'Banana'}, {name: 'Squeak'}]
        u.push 'pets', new Dog name: 'Pogo'
        pets = u.get('pets')
        pets.should.have.length 3
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'
        pets[2].get('name').should.equal 'Pogo'

      it 'pushing a SchemaB instance onto a [SchemaA] field should raise an error', ->
        u = new User name: 'Brian'
        didErr = false
        errMsg = null
        try
          u.push 'pets', new User name: 'Banana'
        catch e
          didErr = true
          errMsg = e.message
        didErr.should.be.true
        errMsg.indexOf('is neither an Object nor a Dog').should.not.equal -1

  describe 'Validation', ->
    it 'should be able to specify a validator inside Schema definition', ->
      Blog = Schema.extend 'User', 'users',
        username:
          $type: String
          validator: (val) ->
            return true if val.length > 7
            return 'Username must be more than 7 characters'

      blog = new Blog username: 'short'
      blog.validate().should.not.be.true

      blog = new Blog username: 'a_valid_username'
      blog.validate().should.be.true

    it 'should be able to specify a validator after initial schema defn', ->
      Blog = Schema.extend 'User', 'users',
        username: String

      Blog.field('username').validator (val) ->
        return true if val.length > 7
        return 'Username must be more than 7 characters'

      blog = new Blog username: 'short'
      blog.validate().should.not.be.true

      blog = new Blog username: 'a_valid_username'
      blog.validate().should.be.true

    it 'validation declared at the type level should be run', ->
      Schema.type 'UsernameA',
        validator: (val) ->
          return true if val.length > 7
          return 'Username must be more than 7 characters'

      Blog = Schema.extend 'User', 'users',
        username: Schema.type 'UsernameA'

      blog = new Blog username: 'short'
      blog.validate().should.not.be.true

      blog = new Blog username: 'a_valid_username'
      blog.validate().should.be.true

  describe 'Type Inheritance', ->
    it 'sub-types should inherit their parent type validators', ->
      Schema.type 'UsernameB',
        validator: (val) ->
          return true if val.length > 7
          return ':fieldName must be more than 7 characters'

      Schema.type 'Email',
        extend: 'UsernameB'
        validator: (val) ->
          return true if /@/.test val
          return ':fieldName must include a @'

      Blog = Schema.extend 'User', 'users',
        username: Schema.type 'Email'

      blog = new Blog username: 'short'
      blog.validate().should.eql [
        ':fieldName must include a @'
        ':fieldName must be more than 7 characters'
      ]

      blog = new Blog username: 'a_valid_username@gmail.com'
      blog.validate().should.be.true

  it 'TODO should test inheritane from grandparent types'

  it 'TODO should interpolate field into validation message'

  describe 'Querying', ->
    # TODO Update this test
    it 'Schema.findById should callback with the appropriate object', (done) ->
      User = Schema.extend 'User', 'users',
        id: Number
        name: String

      # User.source Mongo

      user = { id: 1, name: 'Brian' }
      # model.set 'users.1', user
      User.create user, (err) ->
        should.equal null, err
        User.findById 1, (err, val) ->
          should.equal null, err
          val.should.equal user
          done()
