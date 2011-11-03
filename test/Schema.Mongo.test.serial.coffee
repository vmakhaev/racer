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

    Blog.createDataSchema mongo,
      _id: mongo.pkey ObjectId
      name: String

    Dog = Schema.extend 'Dog', 'dogs',
      _id: String
      name: String

    Dog.createDataSchema mongo, false,
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
      blogs: [Blog]
      tweets: ['Tweet']
    # blog: Blog
    #  friends: [schema('User')]
    #  group: schema('Group')

    # TODO Test using legacy namespaces where logical schema ns != data schema ns
    User.createDataSchema mongo,
      _id: mongo.pkey ObjectId
      name: String
      age: Number
      tags: [String]
      keywords: [String]
      pet: mongo.Dog
      pets: [mongo.Dog]

      blogs: [mongo.Blog.field '_id']
      # pet: { _id: ObjectId, name: String} # TODO Object literals in Schemas
      # TODO Get DataQuery descriptor working
      # TODO Get DataQuery descriptor working
#      blogs: arrayRef (getter, setter) ->
#        getter (blogIds) -> mongo.Blog.find _id: blogIds
#        setter (blogIds) ->
    #  friendIds: [mongo.User.field '_id']
    #  groupId: mongo.schema(Group)._id
#    ,
#      tweets: mongo.Tweet.where('author', '@user._id')
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

    Tweet.createDataSchema mongo, 'tweets',
      _id: mongo.pkey ObjectId
      status: String
      author: mongo.User.field '_id'
      # author: mongo.pointsTo User, '_id'

    Group = Schema.extend 'Group', 'groups',
      _id: String
      name: String
    #  users: [User]


    Group.createDataSchema mongo, 'groups',
      _id: mongo.pkey ObjectId
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
    # User.createDataSchema mongo, 'los_users',
    #   _id: Mongo.pkey
    #   # ...
    #   friendIds: [User._id]
    #   blogId: Blog._id
    # ,
    #   friends: friendIds
    # 
    # # Scen A - array of refs
    # Blog.createDataSchema mongo,
    #   _id: ObjectId
    #   authorIds: [User._id]
    # ,
    #   authors: 'authorIds'
    # 
    # User.createDataSchema mongo,
    #   _id: ObjectId
    # ,
    #   blog: pointedToBy(Blog.authors)
    # 
    # # Scen B - ref
    # Blog.createDataSchema mongo,
    #   _id: ObjectId
    # User.createDataSchema mongo,
    #   _id: ObjectId
    #   blogId: Blog._id
    # ,
    #   blog: 'blogId'
    # 
    # # Scen C - Inverse ref
    # Blog.createDataSchema mongo,
    #   _id: ObjectId
    #   authorId: schema('User')._id # (*)
    # 
    # User.createDataSchema mongo,
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
    User.create name: 'Brian', age: 26, (err, userOne) ->
      should.equal null, err
      User.create name: 'Brian', age: 27, (err, userTwo) ->
        should.equal null, err
        User.find name: 'Brian', (err, found) ->
          should.equal null, err
          found.length.should.equal 2
          for attr in ['_id', 'name', 'age']
            found[0].get(attr).should.equal userOne.get(attr)
            found[1].get(attr).should.equal userTwo.get(attr)
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
  and set as an object literal''': (done) ->
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
      User.findOne {_id}, {select: ['_id', 'name', 'pet']}, (err, foundUser) ->
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

  # Embedded Array of Documents
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
      User.findOne {_id}, {select: ['_id', 'pets']}, (err, foundUser) ->
        should.equal null, err
        pets = foundUser.get 'pets'
        for pet in pets
          pet.should.be.an.instanceof Dog
        pets[0].get('name').should.equal 'Banana'
        pets[1].get('name').should.equal 'Squeak'
        done()

  # Refs
  '''should properly persist a relation specified as a ref and assigned
  as an object literal as (a) an ObjectId and (b) the object identified
  by that ObjectId''': (done) ->
    oplog = []
    Tweet.create
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
            author: authorId
          done()
    , oplog

  '''should properly persist a relation specified as a ref and assigned
  as a Schema doc as (a) an ObjectId and (b) the object identified
  by that ObjectId''': (done) ->
    oplog = []
    author = new User name: 'the clown', true, oplog
    Tweet.create
      author: author
    , (err, tweet) ->
      should.equal null, err
      foundAuthor = tweet.get 'author'
      authorId = ObjectId.fromString foundAuthor.get '_id'
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
            author: authorId
          done()
    , oplog

  '''should record a relation as an ObjectId when assigned to an already
  persisted Schema documents''': (done) ->
    oplog = []
    User.create name: 'the clown', (err, createdAuthor) ->
      should.equal null, err
      Tweet.create
        author: createdAuthor
      , (err, tweet) ->
        should.equal null, err
        tweetId = ObjectId.fromString tweet.get '_id'
        mongo.adapter.findOne 'tweets', _id: tweetId, {}, (err, json) ->
          should.equal null, err
          authorId = ObjectId.fromString createdAuthor.get '_id'
          json.should.eql
            _id: tweetId
            author: authorId
          done()
      , createdAuthor.oplog
    , oplog

  '''should properly persist a relation (+ other fields) specified as a ref and assigned
  as an object literal as (a) an ObjectId and (b) the object identified
  by that ObjectId''': (done) ->
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

  '''should properly persist a relation (+ other fields) specified as a ref and assigned
  as a Schema doc as (a) an ObjectId and (b) the object identified
  by that ObjectId''': (done) ->
    oplog = []
    author = new User name: 'the clown', true, oplog
    Tweet.create
      status: 'why so serious?'
      author: author
    , (err, tweet) ->
      should.equal null, err
      foundAuthor = tweet.get 'author'
      authorId = ObjectId.fromString foundAuthor.get '_id'
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

  '''should record a relation (+ other fields) as an ObjectId when assigned to an already
  persisted Schema documents''': (done) ->
    oplog = []
    User.create name: 'the clown', (err, createdAuthor) ->
      should.equal null, err
      Tweet.create
        status: 'why so serious?',
        author: createdAuthor
      , (err, tweet) ->
        should.equal null, err
        tweetId = ObjectId.fromString tweet.get '_id'
        mongo.adapter.findOne 'tweets', _id: tweetId, {}, (err, json) ->
          should.equal null, err
          authorId = ObjectId.fromString createdAuthor.get '_id'
          json.should.eql
            _id: tweetId
            status: 'why so serious?'
            author: authorId
          done()
      , createdAuthor.oplog
    , oplog

  '''should be able to properly retrieve an ObjectId Ref as the
  configured local schema relation: Schema''': (done) ->
    oplog = []
    Tweet.create
      status: 'why so serious?',
      author: {name: 'the clown'}
    , (err, tweet) ->
      should.equal null, err
      tweetId = tweet.get '_id'
      Tweet.findOne _id: tweetId, {select: ['_id', 'status', 'author']}, (err, foundTweet) ->
        should.equal null, err
        author = foundTweet.get 'author'
        author.should.be.an.instanceof User
        author.get('name').should.equal 'the clown'
        done()
    , oplog

  # Array Refs
  '''should properly persist a relation specified as an array ref as (a) the
  ObjectIds and (b) the objects identified by these ObjectIds''': (done) ->
    oplog = []
    blogsAttrs = [{name: 'Blogorama'}, {name: 'Nom Nom Nom'}]
    User.create
      blogs: blogsAttrs
    , (err, user) ->
      should.equal null, err
      blogs = user.get 'blogs'
      blogIds = (ObjectId.fromString blog.get '_id' for blog in blogs)
      userId = ObjectId.fromString user.get '_id'
      remaining = 1 + blogIds.length
      mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
        should.equal null, err
        json.should.eql
          _id: userId
          blogs: blogIds
        --remaining || done()
      for blogId, i in blogIds
        do (blogId, i) ->
          mongo.adapter.findOne 'blogs', _id: blogId, {}, (err, json) ->
            json.should.eql
              _id: blogId
              name: blogsAttrs[i].name
            --remaining || done()
    , oplog

  '''should record a relation specified as an array ref as ObjectIds when
  assigned to an array of already persisted Schema documents''': (done) ->
    # TODO When oplog is passed in multiple times via nested creates, we end up preserving the ops, when we shouldn't be
    #      Figure out a less error-prone api
    oplog = []
    Blog.create name: 'Blogorama', (err, blogA) ->
      should.equal null, err
      Blog.create name: 'Nom Nom Nom', (err, blogB) ->
        should.equal null, err
        User.create blogs: [blogA, blogB], (err, user) ->
          should.equal null, err
          userId = ObjectId.fromString user.get '_id'
          mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
            should.equal null, err
            blogIds = (ObjectId.fromString doc.get '_id' for doc in [blogA, blogB])
            json.should.eql
              _id: userId
              blogs: blogIds
            done()
        , blogB.oplog
      , blogA.oplog
    , oplog

  '''should be able to properly handle assigning an array ref field a mixed
  array of both persisted and to-be-persisted Schema documents''': (done) ->
    oplog = []
    Blog.create name: 'Blogorama', (err, blogA) ->
      should.equal null, err
      # TODO Again! Having to pass oplog in is very error prone. I forgot again this time. See TODO in the last test. Need a better api
      User.create blogs: [blogA, {name: 'Nom Nom Nom'}, new Blog({name: 'Random Tumblr Blog'}, true, blogA.oplog)], (err, user) ->
        should.equal null, err
        userId = ObjectId.fromString user.get '_id'
        mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
          should.equal null, err
          json.blogs[0].should.eql ObjectId.fromString blogA.get('_id')
          blogIdB = json.blogs[1]
          blogIdC = json.blogs[2]
          mongo.adapter.findOne 'blogs', _id: blogIdB, {}, (err, blogB) ->
            blogB._id.should.eql blogIdB
            blogB.name.should.eql 'Nom Nom Nom'
            mongo.adapter.findOne 'blogs', _id: blogIdC, {}, (err, blogC) ->
              blogC._id.should.eql blogIdC
              blogC.name.should.eql 'Random Tumblr Blog'
              done()
      , blogA.oplog
    , oplog

  '''should properly persist a relation (+ other fields) specified as an array ref as (a) the
  ObjectIds and (b) the objects identified by these ObjectIds''': (done) ->
    oplog = []
    blogsAttrs = [{name: 'Blogorama'}, {name: 'Nom Nom Nom'}]
    User.create
      name: 'Mr. Rogers'
      blogs: blogsAttrs
    , (err, user) ->
      should.equal null, err
      blogs = user.get 'blogs'
      blogIds = (ObjectId.fromString blog.get '_id' for blog in blogs)
      userId = ObjectId.fromString user.get '_id'
      remaining = 1 + blogIds.length
      mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
        should.equal null, err
        json.should.eql
          _id: userId
          name: 'Mr. Rogers'
          blogs: blogIds
        --remaining || done()
      for blogId, i in blogIds
        do (blogId, i) ->
          mongo.adapter.findOne 'blogs', _id: blogId, {}, (err, json) ->
            json.should.eql
              _id: blogId
              name: blogsAttrs[i].name
            --remaining || done()
    , oplog

  '''should record a relation (+ other fields) specified as an array ref as ObjectIds when
  assigned to an array of already persisted Schema documents''': (done) ->
    #      Figure out a less error-prone api
    oplog = []
    Blog.create name: 'Blogorama', (err, blogA) ->
      should.equal null, err
      Blog.create name: 'Nom Nom Nom', (err, blogB) ->
        should.equal null, err
        User.create name: 'Turtle', blogs: [blogA, blogB], (err, user) ->
          should.equal null, err
          userId = ObjectId.fromString user.get '_id'
          mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
            should.equal null, err
            blogIds = (ObjectId.fromString doc.get '_id' for doc in [blogA, blogB])
            json.should.eql
              _id: userId
              name: 'Turtle'
              blogs: blogIds
            done()
        , blogB.oplog
      , blogA.oplog
    , oplog

  '''should be able to properly handle assigning (other fields and) an array ref field a mixed
  array of both persisted and to-be-persisted Schema documents''': (done) ->
    oplog = []
    Blog.create name: 'Blogorama', (err, blogA) ->
      should.equal null, err
      # TODO Again! Having to pass oplog in is very error prone. I forgot again this time. See TODO in the last test. Need a better api
      User.create name: 'Brogrammer', blogs: [blogA, {name: 'Nom Nom Nom'}, new Blog({name: 'Random Tumblr Blog'}, true, blogA.oplog)], (err, user) ->
        should.equal null, err
        userId = ObjectId.fromString user.get '_id'
        mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
          should.equal null, err
          json.name.should.equal 'Brogrammer'
          json.blogs[0].should.eql ObjectId.fromString blogA.get('_id')
          blogIdB = json.blogs[1]
          blogIdC = json.blogs[2]
          mongo.adapter.findOne 'blogs', _id: blogIdB, {}, (err, blogB) ->
            blogB._id.should.eql blogIdB
            blogB.name.should.eql 'Nom Nom Nom'
            mongo.adapter.findOne 'blogs', _id: blogIdC, {}, (err, blogC) ->
              blogC._id.should.eql blogIdC
              blogC.name.should.eql 'Random Tumblr Blog'
              done()
      , blogA.oplog
    , oplog

  '''should properly order the pkeys in an array ref field, not in the order of array ref
  member doc creation in a single oplog''': (done) ->
    oplog = []
    blogA = new Blog name: 'Blogorama', true, oplog
    blogB = new Blog name: 'Nom Nom Nom', true, oplog
    User.create blogs: [blogB, blogA], (err, user) ->
      should.equal null, err
      userId = ObjectId.fromString user.get '_id'
      mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
        should.equal null, err
        json.blogs[0].should.eql ObjectId.fromString blogB.get('_id')
        json.blogs[1].should.eql ObjectId.fromString blogA.get('_id')
        done()
    , oplog

  '''should be able to properly retrieve an [ObjectId] Array Ref as the
  configured local schema relation: [Schema]''': (done) ->
    oplog = []
    blogsAttrs = [{name: 'Blogorama'}, {name: 'Nom Nom Nom'}]
    User.create
      blogs: blogsAttrs
    , (err, user) ->
      should.equal null, err
      userId = user.get '_id'
      User.findOne _id: userId, {select: ['_id', 'blogs']}, (err, foundUser) ->
        should.equal null, err
        blogs = foundUser.get 'blogs'
        for blog, i in blogs
          blog.should.be.an.instanceof Blog
          blog.get('name').should.equal blogsAttrs[i].name
        done()
    , oplog

  # Inverse Refs as an Array
#  '''should properly persist a relation that is the collection of documents that
#  point to me via a Ref field in their schemas @single''': (done) ->
#    oplog = []
#    tweetsAttrs = [
#      { status: 'hasta' }
#      { status: 'la' }
#      { status: 'vista' }
#      { status: 'baby' }
#    ]
#    User.create
#      name: 'Brian'
#      tweets: tweetAttrs
#    , (err, user) ->
#      should.equal null, err
#      userId = ObjectId.fromString user.get '_id'
#      blogs = user.get 'blogs'
#      blogIds = (ObjectId.fromString blog.get '_id' for blog in blogs)
#      remaining = 1 + blogIds.length
#      mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
#        Object.keys(json).length.should.equal 2
#        json._id.should.not.be.undefined
#        json.name.should.not.be.undefined
#        should.equal undefined, json._id
#        --remaining || done()
#      for blogId, i in blogIds
#        do (i) ->
#          mongo.adapter.findOne 'blogs', _id: blogId, {}, (err, json) ->
#            json.status.should.equal tweetsAttrs[i].status
#            --remaining || done()
#    , oplog
#
#  '''should properly retrieve a relation that is the collection of documents that
#  point to me via a Ref field in their schemas @single''': (done) ->
#    oplog = []
#    tweetsAttrs = [
#      { status: 'hasta' }
#      { status: 'la' }
#      { status: 'vista' }
#      { status: 'baby' }
#    ]
#    User.create
#      name: 'Brian'
#      tweets: tweetAttrs
#    , (err, user) ->
#      should.equal null, err
#      userId = user.get '_id'
#      User.findOne _id: userId, {select: ['_id', 'tweets']}, (err, foundUser) ->
#        should.equal null, err
#        tweets = foundUser.get 'tweets'
#        for tweet, i in tweets
#          tweet.should.be.an.instanceof Tweet
#          tweet.get('status').should.equal tweetsAttrs[i].status
#        done()
#    , oplog

  # Command building
  'should create a new update $set command for a single set': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.set 'name', 'Brian'
    commandSet = Schema._oplogToCommandSet u.oplog
    cmd = commandSet.singleCommand
    {method, args} = cmd.compile()
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian' } }
      { upsert: true, safe: true }
    ]
    done()

  '''should add a 2nd set to an existing update $set command
  after a 1st set generates that command''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.set 'name', 'Brian'
    u.set 'age', 26
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian', age: 26 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new $unset command for a single del': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.del 'name'
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $unset: { name: 1 } }
      { upsert: true, safe: true }
    ]
    done()

  '''a 2nd sequential del on the same schema doc but different
  field should add the field to the existing update $unset command
  involving the first del's field''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.del 'name'
    u.del 'age'
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $unset: { name: 1, age: 1 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new update $push command for a single push': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs'
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]
    done()

  '''pushing multiple items with a single push should result
  in a $pushAll command''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs', 'sf'
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]
    done()

  '2 pushes should result in a $pushAll': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs'
    u.push 'tags', 'sf'
    cmdSet = Schema._oplogToCommandSet u.oplog
    cmd = cmdSet.singleCommand
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]
    done()

  '''a single item push on field A, followed by a single item
  push on field B should result in 2 $push commands''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs'
    u.push 'keywords', 'sf'
    cmdSet = Schema._oplogToCommandSet u.oplog

    cmdIds = Object.keys cmdSet.commandsById
    cmdIds.should.have.length 2

    cmd = cmdSet.commandsById[cmdIds[0]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    cmd = cmdSet.commandsById[cmdIds[1]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { keywords: 'sf'} }
      { upsert: true, safe: true }
    ]

    done()

  '''a single item push on field A, followed by a multi item
  push on field B should result in 1 $push command and 1 $pushAll
  command''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs'
    u.push 'keywords', 'sf', 'socal'
    cmdSet = Schema._oplogToCommandSet u.oplog

    cmdIds = Object.keys cmdSet.commandsById
    cmdIds.should.have.length 2

    cmd = cmdSet.commandsById[cmdIds[0]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    cmd = cmdSet.commandsById[cmdIds[1]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { keywords: ['sf', 'socal']} }
      { upsert: true, safe: true }
    ]

    done()

  '''a multi item push on field A, followed by a single item
  push on field B should result in 1 $pushAll command and 1 $push
  command''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.push 'tags', 'nodejs', 'sf'
    u.push 'keywords', 'socal'
    cmdSet = Schema._oplogToCommandSet u.oplog

    cmdIds = Object.keys cmdSet.commandsById
    cmdIds.should.have.length 2

    cmd = cmdSet.commandsById[cmdIds[0]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]

    cmd = cmdSet.commandsById[cmdIds[1]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { keywords: 'socal'} }
      { upsert: true, safe: true }
    ]

    done()

  '''a set on field A followed by a single item push on field B
  should result in 1 $set and 1 $push command''': (done) ->
    isNew = false
    u = new User _id: 1, isNew
    u.set 'name', 'Brian'
    u.push 'keywords', 'socal'
    cmdSet = Schema._oplogToCommandSet u.oplog

    cmdIds = Object.keys cmdSet.commandsById
    cmdIds.should.have.length 2

    cmd = cmdSet.commandsById[cmdIds[0]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $set: { name: 'Brian' } }
      { upsert: true, safe: true }
    ]

    cmd = cmdSet.commandsById[cmdIds[1]]
    {method, args} = cmd.compile()
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
    sharedOplog = []
    isNew = false
    u1 = new User _id: 1, isNew, sharedOplog
    u2 = new User _id: 2, isNew, sharedOplog
    u1.push 'tags', 'nodejs'
    u2.push 'tags', 'sf'

    cmdSet = Schema._oplogToCommandSet sharedOplog

    cmdIds = Object.keys cmdSet.commandsById
    cmdIds.length.should.equal 2

    cmd = cmdSet.commandsById[cmdIds[0]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 1}
      { $push: { tags: 'nodejs' } }
      { upsert: true, safe: true }
    ]

    cmd = cmdSet.commandsById[cmdIds[1]]
    {method, args} = cmd.compile()
    method.should.equal 'update'
    args.should.eql [
      'users'
      {_id: 2}
      { $push: { tags: 'sf'} }
      { upsert: true, safe: true }
    ]

    done()

  # TODO Add tests for change of isNew true -> false
