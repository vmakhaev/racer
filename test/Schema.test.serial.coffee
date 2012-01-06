should = require 'should'
Schema = require '../src/Schema/Logical/Schema'

module.exports =
  '''Schema.extend should return a contructor that inherits
  from Schema''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String
    user = new User
    user.should.be.an.instanceof User
    user.should.be.an.instanceof Schema
    done()

  'a Schema subclass should be able to extend itelf': (done) ->
    User = Schema.extend 'User', 'users',
      name: String
    Admin = User.extend 'Admin', 'admins',
      privileges: [String]

    admin = new Admin
    admin.should.be.an.instanceof Admin
    admin.should.be.an.instanceof User
    admin.should.be.an.instanceof Schema
    done()

  'a Schema subclass should inherit static methods': (done) ->
    User = Schema.extend 'User', 'users',
      name: String

    counter = 0
    User.static 'newStatic', ->
      counter++
    Admin = User.extend 'Admin', 'admins',
      privileges: [String]

    Admin.newStatic()
    counter.should.equal 1

    done()

  '''adding a static to a Schema should add that static to
  all descendant Schemas''': (done) ->
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

    done()

  '''adding a static to a Schema should not add that method
  to ancestor Schemas''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String

    counter = 0

    Admin = User.extend 'Admin', 'admins',
      privileges: [String]

    Admin.static 'newStatic', true

    Admin.newStatic.should.not.be.undefined
    should.equal undefined, User.newStatic
    should.equal undefined, Schema.newStatic
    done()

  '''changing a parent static after assigning a child static
  of the same name should not over-write the child static''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String

    counter = 0

    Admin = User.extend 'Admin', 'admins',
      privileges: [String]

    Admin.static 'newStatic', 'super'

    User.static 'newStatic', 'ballin'

    User.newStatic.should.equal 'ballin'
    Admin.newStatic.should.equal 'super'
    done()

  '''Schema.fromPath should return the schema and path remainder
  for an absolute path''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String

    {Skema, path, id} = Schema.fromPath 'users.1.name'
    Skema.should.equal User
    id.should.equal '1'
    path.should.equal 'name'
    done()

  '''an instantiated Schema doc initialized with attributes
  should be able to retrieve those attributes via get''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String
    user = new User name: 'Brian'
    user.get('name').should.equal 'Brian'
    done()

  # Casting
  'logical schema layer should cast String attributes': (done) ->
    User = Schema.extend 'User', 'users',
      _id: String
    s = new User _id: 1
    s.get('_id').should.equal '1'
    done()

  'logical schema layer should cast Number attributes': (done) ->
    User = Schema.extend 'User', 'users',
      age: Number
    s = new User age: '26'
    s.get('age').should.equal 26
    done()

  'logical schema layer should cast [String] attributes': (done) ->
    User = Schema.extend 'User', 'users',
      tags: [String]
    u = new User tags: [1, 2, 3, 4]
    u.get('tags').should.eql ['1', '2', '3', '4']
    done()

  'logical schema layer should cast [Number] attributes': (done) ->
    User = Schema.extend 'User', 'users',
      luckyNumbers: [Number]
    s = new User luckyNumbers: ['4', 8, '12', 16]
    s.get('luckyNumbers').should.eql [4, 8, 12, 16]
    done()

  'logical schema layer should cast CustomSchema attributes': (done) ->
    Blog = Schema.extend 'Blog', 'blogs',
      _id: String
      name: String
    User = Schema.extend 'User', 'users',
      blog: Blog
    s = new User blog: { _id: 5, name: 'Racer Blog' }
    blog = s.get('blog')
    blog.should.be.an.instanceof Blog
    blog.get('_id').should.equal '5'
    blog.get('name').should.equal 'Racer Blog'
    done()

  'logical schema layer should cast [CustomSchema] attributes': (done) ->
    Dog = Schema.extend 'Dog', 'dogs',
      _id: String
      name: String
      age: Number
    User = Schema.extend 'Blog', 'blogs',
      dogs: [Dog]

    u = new User dogs: [
      { _id: 1, name: 'Banana', age: '2'}
      {_id: 2, name: 'Squeak', age: '8'}
    ]
    dogs = u.get 'dogs'
    dogs.length.should.equal 2
    for dog in dogs
      dog.should.be.an.instanceof Dog
    dogs[0].get('name').should.equal 'Banana'
    dogs[0].get('age').should.equal 2
    dogs[1].get('name').should.equal 'Squeak'
    dogs[1].get('age').should.equal 8
    done()

  # Validation
  'should be able to specify a validator inside Schema definition': (done) ->
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
    done()

  'should be able to specify a validator after initial schema defn': (done) ->
    Blog = Schema.extend 'User', 'users',
      username: String

    Blog.field('username').validator (val) ->
      return true if val.length > 7
      return 'Username must be more than 7 characters'

    blog = new Blog username: 'short'
    blog.validate().should.not.be.true

    blog = new Blog username: 'a_valid_username'
    blog.validate().should.be.true
    done()

  'validation declared at the type level should be run': (done) ->
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
    done()

  # Type Inheritance
  'sub-types should inherit their parent type validators': (done) ->
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
    done()

  # TODO test inheritance from grandparent types

  # TODO interpolate field into validation message

  # Querying
  'Schema.findById should callback with the appropriate object': (done) ->
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
