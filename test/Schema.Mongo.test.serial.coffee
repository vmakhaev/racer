should = require 'should'
Mongo = require 'Schema/Mongo'
mongo = null
Schema = require 'Schema'
Blog = null
Dog = null
User = null
Group = null
Tweet = null
ObjectId = null

module.exports =
  setup: (done) ->
    mongo = new Mongo
    {ObjectId} = require 'Schema/Mongo/types'

    # Reset this static variable, to keep tests independent
    Schema._sources = []

    Blog = Schema.extend 'Blog', 'blogs',
      _id: String
      name: String

    Dog = Schema.extend 'Dog', 'dogs',
      _id: String
      name: String

    Dog.source mongo, 'dogs',
      _id: ObjectId
      name: String

    User = Schema.extend 'User', 'users',
      _id: String
      name: String
      age: Number
      tags: [String]
      keywords: [String]
      luckyNumbers: [Number]
      pet: Dog
      pets: [Dog]
    # blog: Blog
    #  friends: [schema('User')]
      # BRIAN: A Schema should be a special Type?
      # But Types only make sense in the context of being used
      # in a Schema definition. So perhaps, it is more accurate
      # to say that Schema can have a Type interface
    #  group: schema('Group')

    # mongo.connect 'mongodb://localhost/racer_test'
    #User.source mongo, 'los_users',
    User.source mongo, 'users',
      _id: mongo.pkey ObjectId
      name: String
      age: Number
      tags: [String]
      keywords: [String]
      pet: Object
      pets: [Object]
    #  friendIds: [User._id]
    #  groupId: schema(Group)._id
    #,
    #  friends: User.friendIds

    # # Alt A
    # friends: User.where('_id').findOne (err, user) ->
    #   User.where('_id', user.friendIds).find()
    # # Alt B
    # friends: (id) -> User.where('_id', User.where('_id', id).findOne().friendIds).find()

    Tweet = Schema.extend 'Tweet', 'tweets',
      _id: String
      status: String
      author: User


    Tweet.source mongo, 'tweets',
      _id: ObjectId
      status: String
      author: mongo.User._id
      # author: mongo.pointsTo User, '_id'

    Group = Schema.extend 'Group', 'groups',
      _id: String
      name: String
    #  users: [User]


    Group.source mongo, 'groups',
      _id: ObjectId
      name: String
    #,
    #  users: [User.groupId]


    #   users: [User.groupId.pointingTo.me]
    #   users: User.find().where('groupId') # Curries fn User.where('groupId', thisGroupId)
    #   users: curry User.find().where, 'groupId'

    #   # Using join table
    #   friends: [schema('FriendsJoin').friendA]
    # 
    # # (*)
    # # users                             users
    # # me <---[friendAId, friendBId]---> me.friends 
    # Mongo.schema.join 'FriendsJoin', 'friends_join',
    #   friendXId: User._id
    #   friendYId: User._id
    # ,
    #   friendA: friendYId
    #   friendB: friendXId
    # 
    # User.source mongo, 'los_users',
    #   _id: Mongo.pkey
    #   # ...
    #   friendIds: [User._id]
    #   blogId: Blog._id
    # ,
    #   friends: friendIds
    # 
    # # Scen A - array of refs
    # Blog.source mongo,
    #   _id: ObjectId
    #   authorIds: [User._id]
    # ,
    #   authors: 'authorIds'
    # 
    # User.source mongo,
    #   _id: ObjectId
    # ,
    #   blog: pointedToBy(Blog.authors)
    # 
    # # Scen B - ref
    # Blog.source mongo,
    #   _id: ObjectId
    # User.source mongo,
    #   _id: ObjectId
    #   blogId: Blog._id
    # ,
    #   blog: 'blogId'
    # 
    # # Scen C - Inverse ref
    # Blog.source mongo,
    #   _id: ObjectId
    #   authorId: schema('User')._id # (*)
    # 
    # User.source mongo,
    #   _id: ObjectId
    # ,
    #   blog: Blog.authorId
    mongo.connect 'mongodb://localhost/racer_test'
    mongo.flush done

  teardown: (done) ->
    mongo.flush ->
      mongo.disconnect ->
        Blog = null
        Dog = null
        User = null
        Group = null
        Tweet = null
        ObjectId = null
        done()

  'primary key _id should be created on saving a new doc': (done) ->
    u = new User name: 'Brian'
    u.save (err, u) ->
      should.equal null, err
      _id = u.get '_id'
      _idType = typeof _id
      _idType.should.equal 'string'
      _id.length.should.equal 24
      done()

  'should be able to retrieve a document after creating it': (done) ->
    User.create name: 'Brian', age: 26, (err, createdUser) ->
      should.equal null, err
      User.findOne
        _id: createdUser.get '_id'
      , (err, foundUser) ->
        should.equal null, err
        for path in ['_id', 'name', 'age']
          foundUser.get(path).should.equal createdUser.get(path)
        foundUser.get('name').should.equal 'Brian'
        foundUser.get('age').should.equal 26
        done()

  'should be able to retrieve > 1 docs after creating them': (done) ->
    User.create name: 'Brian', (err, userOne) ->
      should.equal null, err
      User.create name: 'Brian', (err, userTwo) ->
        should.equal null, err
        User.find name: 'Brian', (err, found) ->
          should.equal null, err
          found.length.should.equal 2
          found[0].get('_id').should.equal userOne.get('_id')
          found[1].get('_id').should.equal userTwo.get('_id')
          done()

  'a found document should not initially have an oplog': (done) ->
    User.create name: 'Brian', age: 26, (err, createdUser) ->
      should.equal null, err
      User.findOne
        _id: createdUser.get '_id'
      , (err, foundUser) ->
        should.equal null, err
        foundUser.oplog.should.be.empty
        done()

  'array of found documents should not initially have an oplog': (done) ->
    User.create name: 'Brian', age: 26, (err, createdUser) ->
      should.equal null, err
      User.create name: 'Brian', age: 26, (err, createdUser) ->
        should.equal null, err
        User.find
          name: 'Brian'
        , (err, found) ->
          should.equal null, err
          for foundUser in found
            foundUser.oplog.should.be.empty
          done()

  'should persist a single push onto a document array field': (done) ->
    u = new User name: 'Brian'
    u.push 'tags', 'nodejs'
    u.save (err, createdUser) ->
      should.equal null, err
      createdUser.get('tags').should.eql ['nodejs']
      User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
        should.equal null, err
        foundUser.get('tags').should.eql ['nodejs']
        done()

  'should persist multiple single pushes onto a document array field': (done) ->
    u = new User name: 'Brian'
    u.push 'tags', 'nodejs'
    u.push 'tags', 'sf'
    u.save (err, createdUser) ->
      should.equal null, err
      createdUser.get('tags').should.eql ['nodejs', 'sf']
      User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
        should.equal null, err
        foundUser.get('tags').should.eql ['nodejs', 'sf']
        done()

  'should persist a single multiple member push onto a document array field': (done) ->
    u = new User name: 'Brian'
    u.push 'tags', 'nodejs', 'sf'
    u.save (err, createdUser) ->
      should.equal null, err
      createdUser.get('tags').should.eql ['nodejs', 'sf']
      User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
        should.equal null, err
        foundUser.get('tags').should.eql ['nodejs', 'sf']
        done()

  # Embedded documents
  # TODO Move this test and other appropriate tests to Schema.test.serial.coffee
  '''setting to an object literal, a field that maps to a Schema should assign
  an instance of that Schema to the document attribute of the same name''': (done) ->
    u = new User name: 'Brian'
    u.set 'pet', name: 'Banana'
    u.get('pet').should.be.an.instanceof Dog
    u.get('pet').get('name').should.equal 'Banana'
    done()

  '''setting to a Schema instance, a field that maps to a Schema should assign
  that Schema instance directly''': (done) ->
    u = new User name: 'Brian'
    u.set 'pet', new Dog name: 'Banana'
    u.get('pet').should.be.an.instanceof Dog
    u.get('pet').get('name').should.equal 'Banana'
    done()

  '''setting to a non-matching Schema instance, a field that maps to another
  Schema should raise an error''': (done) ->
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
    done()


  '''should properly persist a relation specified as an embedded document
  and set as an object literal @single''': (done) ->
    u = new User name: 'Brian'
    u.set 'pet', name: 'Banana'
    u.save (err, createdUser) ->
      should.equal null, err
      _id = ObjectId.fromString createdUser.get '_id'
      # dogId = createdUser.get('pet').get('_id')
      mongo.adapter.findOne 'users', {_id}, {}, (err, json) ->
        should.equal null, err
        json.should.eql
          _id: _id
          name: 'Brian'
          pet:
            # _id: dogId
            name: 'Banana'
        done()

  '''should be able to properly retrieve an embedded document
  as the configured logical schema relation''': (done) ->
    u = new User name: 'Brian'
    u.set 'pet', name: 'Banana'
    u.save (err, createdUser) ->
      should.equal null, err
      _id = createdUser.get '_id'
      User.findOne {_id}, (err, foundUser) ->
        should.equal null, err
        dog = foundUser.get 'pet'
        dog.should.be.an.instanceof Dog
        dog.get('name').should.equal 'Banana'
        done()

  # Embedded Array documents
  '''setting to an array of object literals, a field that maps to [Schema] should assign
  an array of Schema instances to the document attribute of the same name''': (done) ->
    u = new User name: 'Brian'
    u.set 'pets', [{name: 'Banana'}, {name: 'Squeak'}]
    pets = u.get('pets')
    pets.should.have.length 2
    for pet in pets
      pet.should.be.an.instanceof Dog
    pets[0].get('name').should.equal 'Banana'
    pets[1].get('name').should.equal 'Squeak'
    done()

  '''setting to an array of CustomSchema instances, a field that maps to
  [Schema] should assign the array of Schema instances directly''': (done) ->
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
    done()

  '''setting to an array of SomeSchema instances that contains a non-matching
  SchemaB, a field that maps to [SchemaA] should raise an error''': (done) ->
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
    done()

  '''pushing an object literal onto a [Schema] field should convert
  the object literal into a Schema document and append it to the attribute''': (done) ->
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
    done()

  '''pushing a Schema instance onto a [Schema] field should push the
  Schema instance directly onto the existing array of instances''': (done) ->
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
    done()

  '''pushing a SchemaB instance onto a [SchemaA] field should raise
  an error''': (done) ->
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
    done()

  '''should persist a relation specified as an embedded array of
  documents as an embedded array of object literals on Mongo''': (done) ->
    u = new User name: 'Brian'
    u.push 'pets', {name: 'Banana'}, {name: 'Squeak'}
    u.save (err) ->
      should.equal null, err
      _id = ObjectId.fromString u.get '_id'
      mongo.adapter.findOne 'users', {_id}, {}, (err, json) ->
        should.equal null, err
        json.should.eql
          _id: _id
          name: 'Brian'
          pets: [
            { name: 'Banana' }
            { name: 'Squeak' }
          ]
        done()

  '''should be able to properly retrieve an embedded array of documents
  as the configured local schema relation [Schema]''': (done) ->
    u = new User name: 'Brian'
    u.push 'pets', {name: 'Banana'}, {name: 'Squeak'}
    u.save (err) ->
      should.equal null, err
      _id = u.get '_id'
      User.findOne {_id}, (err, foundUser) ->
        should.equal null, err
        pets = foundUser.get 'pets'
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'
        done()

  # Refs
  '''should properly persist a relation specified as a ref as (a) an
  ObjectId and (b) the object identified by that ObjectId''': (done) ->
    oplog = []
    Tweet.create
      status: 'why so serious?',
      author: {name: 'the clown'}
    , (err, tweet) ->
      should.equal null, err
      author = tweet.get 'author'
      authorId = ObjectId.fromString author.get '_id'
      tweetId = ObjectId.fromString tweet.get '_id'
      mongo.adapter.findOne 'users', _id: authorId, {}, (err, json) ->
        should.equal null, err
        json.should.eql
          _id: authorId
          name: 'the clown'
        mongo.adapter.findOne 'tweets', _id: tweetId, {}, (err, json) ->
          should.equal null, err
          json.should.eql
            _id: tweetId
            status: 'why so serious?'
            author: authorId
          done()
    , oplog

  # Query building
  'should create a new update $set query for a single set': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.set 'name', 'Brian'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian' } }
      { upsert: true, safe: true }
    ]
    done()

  '''should add a 2nd set to an existing update $set query
  after a 1st set generates that query''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.set 'name', 'Brian'
    s.set 'age', 26
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian', age: 26 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new $unset query for a single del': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.del 'name'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $unset: { name: 1 } }
      { upsert: true, safe: true }
    ]
    done()

  '''a 2nd sequential del on the same schema doc but different
  field should add the field to the existing update $unset query
  involving the first del's field''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.del 'name'
    s.del 'age'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $unset: { name: 1, age: 1 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new update $push query for a single push': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]
    done()

  '''pushing multiple items with a single push should result
  in a $pushAll query''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs', 'sf'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]
    done()

  '2 pushes should result in a $pushAll': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs'
    s.push 'tags', 'sf'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]
    done()

  '''a single item push on field A, followed by a single item
  push on field B should result in 2 $push queries''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs'
    s.push 'keywords', 'sf'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { keywords: 'sf'} }
      { upsert: true, safe: true }
    ]

    done()

  '''a single item push on field A, followed by a multi item
  push on field B should result in 1 $push query and 1 $pushAll
  query''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs'
    s.push 'keywords', 'sf', 'socal'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { keywords: ['sf', 'socal']} }
      { upsert: true, safe: true }
    ]

    done()

  '''a multi item push on field A, followed by a single item
  push on field B should result in 1 $pushAll query and 1 $push
  query''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs', 'sf'
    s.push 'keywords', 'socal'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { keywords: 'socal'} }
      { upsert: true, safe: true }
    ]

    done()

  '''a set on field A followed by a single item push on field B
  should result in 1 $set and 1 $push query''': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.set 'name', 'Brian'
    s.push 'keywords', 'socal'
    queries = mongo._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian' } }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { keywords: 'socal'} }
      { upsert: true, safe: true }
    ]

    done()

  '''an oplog involving 2 different conditions should result in 2 separate
  queries for oplog single-item push on doc matching conditions A, then
  single-item push on doc matching conditions B''': (done) ->
    addOp = false
    s1 = new User _id: 1, addOp
    s2 = new User _id: 2, addOp
    s1.push 'tags', 'nodejs'
    s2.push 'tags', 'sf'
    oplog = s1.oplog.concat s2.oplog

    queries = mongo._queriesForOps oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs' } }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 2}
      { $push: { tags: 'sf'} }
      { upsert: true, safe: true }
    ]

    done()

  # TODO Add tests for change of isNew true -> false
