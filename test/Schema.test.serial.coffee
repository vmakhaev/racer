should = require 'should'
Schema = require 'Schema'

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

    {schema, path} = Schema.fromPath 'users.1.name'
    schema.should.equal User
    path.should.equal '1.name'
    done()

  '''an instantiated Schema doc initialized with attributes
  should be able to retrieve those attributes via get''': (done) ->
    User = Schema.extend 'User', 'users',
      name: String
    user = new User name: 'Brian'
    user.get('name').should.equal 'Brian'
    done()

  'Schema.findById should callback with the appropriate object': (done) ->
    User = Schema.extend 'User', 'users',
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
