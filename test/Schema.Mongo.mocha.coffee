should = require 'should'
Mongo = require '../src/Schema/DataSource/Mongo'
Schema = require '../src/Schema/Logical/Schema'
{ObjectId} = require '../src/Schema/DataSource/Mongo/types'
CommandSequence = require '../src/Schema/CommandSequence'

describe 'Schema document', ->
  mongo = new Mongo

  # Reset these static variables for test independence
  Schema._sources = []
  Schema._schemas = {}
  Schema._schemaPromises = {}

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
    tweets: ['Tweet'] # many('Tweet', inverse: 'author')
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
  ,
    tweets: mongo.schema('Tweet').find().where('author', '@user._id')
  #  friends: User.friendIds

  # # Alt A
  # friends: User.where('_id').findOne (err, user) ->
  #   User.where('_id', user.friendIds).find()
  # # Alt B
  # friends: (id) -> User.where('_id', User.where('_id', id).findOne().friendIds).find()

  Tweet = Schema.extend 'Tweet', 'tweets',
    _id: String
    status: String
    author: User # one(User, inverse: 'tweets')

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
  # TODO Test using legacy namespaces where logical schema ns != data schema ns
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

  before (done) -> mongo.flush done

  after (done) -> mongo.flush -> mongo.disconnect done

  describe 'saving a new doc', ->
    beforeEach (done) -> mongo.flush done

    afterEach (done) -> mongo.flush done

    it 'should create a primary key', (done) ->
      u = new User name: 'Brian'
      u.save (err, u) ->
        should.equal null, err
        _id = u.get '_id'
        _idType = typeof _id
        _idType.should.equal 'string'
        _id.length.should.equal 24
        done()

  describe 'querying', ->
    userA = null
    userB = null
    before (done) ->
      mongo.flush ->
        rem = 2
        User.create name: 'Brian', age: 26, (err, user) ->
          userA = user
          --rem || done(err, user)
        User.create name: 'Brian', age: 27, (err, user) ->
          userB = user
          --rem || done(err, user)
    after (done) ->
      mongo.flush done
    describe '#findOne', ->
      it 'should retrieve a matching doc', (done) ->
        User.findOne _id: userA.get('_id'), (err, foundUser) ->
          for path in ['_id', 'name', 'age']
            foundUser.get(path).should.equal userA.get(path)
          foundUser.get('name').should.equal 'Brian'
          foundUser.get('age').should.equal 26
          done err

      it 'should retrieve null if no match', (done) ->
        User.findOne age: 1000, (err, foundUser) ->
          should.equal null, foundUser
          done err

      it 'should retrieve a doc with an empty oplog', (done) ->
        User.findOne _id: userA.get('_id'), (err, foundUser) ->
          foundUser.oplog.should.be.empty
          done err

    describe '#find', ->
      it 'should retrieve > 1 matching docs', (done) ->
        User.find name: 'Brian', (err, found) ->
          found.length.should.equal 2
          for attr in ['_id', 'name', 'age']
            found[0].get(attr).should.equal userA.get(attr)
            found[1].get(attr).should.equal userB.get(attr)
          done err

      it 'should retrieve an array of docs with an empty oplog', (done) ->
        User.find name: 'Brian', (err, found) ->
          for foundUser in found
            foundUser.oplog.should.be.empty
          done err

  describe 'pushing onto an array field', ->
    beforeEach (done) -> mongo.flush done
    it 'should persist a single push', (done) ->
      u = new User name: 'Brian'
      u.push 'tags', 'nodejs'
      u.save (err, createdUser) ->
        should.equal null, err
        createdUser.get('tags').should.eql ['nodejs']
        User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
          should.equal null, err
          foundUser.get('tags').should.eql ['nodejs']
          done err

    it 'should persist multiple pushes', (done) ->
      u = new User name: 'Brian'
      u.push 'tags', 'nodejs'
      u.push 'tags', 'sf'
      u.save (err, createdUser) ->
        should.equal null, err
        createdUser.get('tags').should.eql ['nodejs', 'sf']
        User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
          should.equal null, err
          foundUser.get('tags').should.eql ['nodejs', 'sf']
          done err

    it 'should persist a single multiple member push', (done) ->
      u = new User name: 'Brian'
      u.push 'tags', 'nodejs', 'sf'
      u.save (err, createdUser) ->
        should.equal null, err
        createdUser.get('tags').should.eql ['nodejs', 'sf']
        User.findOne _id: createdUser.get('_id'), (err, foundUser) ->
          should.equal null, err
          foundUser.get('tags').should.eql ['nodejs', 'sf']
          done err

  describe 'a field that maps to a Schema - aka embedded doc fields', ->
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

    describe 'persisting', ->
      beforeEach (done) -> mongo.flush done
      it 'should properly persist a set to an object literal', (done) ->
        u = new User name: 'Brian'
        u.set 'pet', name: 'Banana'
        u.save (err, createdUser) ->
          should.equal null, err
          _id = ObjectId.fromString createdUser.get '_id'
          # dogId = createdUser.get('pet').get('_id')
          mongo.adapter.findOne 'users', {_id}, {}, (err, json) ->
            json.should.eql
              _id: _id
              name: 'Brian'
              pet:
                # _id: dogId
                name: 'Banana'
            done err
      it 'should be able to retrieve the embedded doc as the configure logical schema relation', (done) ->
        u = new User name: 'Brian'
        u.set 'pet', name: 'Banana'
        u.save (err, createdUser) ->
          should.equal null, err
          _id = createdUser.get '_id'
          User.findOne {_id}, {select: ['_id', 'name', 'pet']}, (err, foundUser) ->
            dog = foundUser.get 'pet'
            dog.should.be.an.instanceof Dog
            dog.get('name').should.equal 'Banana'
            done err

  describe 'a field that maps to [Schema] - aka embedded array doc fields', ->
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

    describe 'persistence', ->
      beforeEach (done) -> mongo.flush done
      it 'should persist the [Schema] relation as an embedded array of object literals in mongo', (done) ->
        u = new User name: 'Brian'
        u.push 'pets', {name: 'Banana'}, {name: 'Squeak'}
        u.save (err) ->
          should.equal null, err
          _id = ObjectId.fromString u.get '_id'
          mongo.adapter.findOne 'users', {_id}, {}, (err, json) ->
            json.should.eql
              _id: _id
              name: 'Brian'
              pets: [
                { name: 'Banana' }
                { name: 'Squeak' }
              ]
            done err

      it 'should be able to properly retrieve an embedded array of docs as the configured logical schema relation [Schema]', (done) ->
        u = new User name: 'Brian'
        u.push 'pets', {name: 'Banana'}, {name: 'Squeak'}
        u.save (err) ->
          should.equal null, err
          _id = u.get '_id'
          User.findOne {_id}, {select: ['_id', 'pets']}, (err, foundUser) ->
            pets = foundUser.get 'pets'
            for pet in pets
              pet.should.be.an.instanceof Dog
            pets[0].get('name').should.equal 'Banana'
            pets[1].get('name').should.equal 'Squeak'
            done err

  describe 'Refs', ->
    beforeEach (done) -> mongo.flush done
    it 'should properly persist a relation specified as a ref and assigned as an object literal as (a) an ObjectId and (b) the object identified by that ObjectId', (done) ->
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

    it 'should persist a ref assgiend as a Schema doc as (a) an ObjectId and (b) the object identified by that ObjectId', (done) ->
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

    it 'should record a ref as an ObjectId when assigned to an already persisted Schema doc', (done) ->
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

    it 'should properly persist a ref (+ other fields) that is assigned as an object literal as (a) an ObjectId and (b) the object identified by that ObjectId', (done) ->
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

    it 'should persist a ref (+ other fields) that is assigned as a Schema doc as (a) an ObjectId and (b) the object identified by that ObjectId', (done) ->
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

    it 'should persist a ref (+ other fields) as an ObjectId when assigned to an already persisted Schema doc', (done) ->
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
            authorId = ObjectId.fromString createdAuthor.get '_id'
            json.should.eql
              _id: tweetId
              status: 'why so serious?'
              author: authorId
            done err
        , createdAuthor.oplog
      , oplog

    it 'should retrieve an ObjectId Ref as the configured local schema relation: Schema', (done) ->
      oplog = []
      Tweet.create
        status: 'why so serious?',
        author: {name: 'the clown'}
      , (err, tweet) ->
        should.equal null, err
        tweetId = tweet.get '_id'
        Tweet.findOne _id: tweetId, {select: ['_id', 'status', 'author']}, (err, foundTweet) ->
          author = foundTweet.get 'author'
          author.should.be.an.instanceof User
          author.get('name').should.equal 'the clown'
          done err
      , oplog

  describe 'Array Ref fields', ->
    beforeEach (done) -> mongo.flush done

    it 'should persist an array ref as (a) the ObjectIds and (b) the objects identified by these ObjectIds', (done) ->
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

    it 'should persist an array ref as ObjectIds when assigned to an array of already persisted Schema docs', (done) ->
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

    it 'should assign an array ref field a mixed array of both persisted and to-be-persisted Schema docs', (done) ->
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

    it 'should persist an array ref (+ other fields) as (a) the ObjectIds and (b) the objects identified by these ObjectIds', (done) ->
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

    it 'should record an array ref (+ other fields_ as ObjectIds when assigned to an array of already persisted Schema docs', (done) ->
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

    it 'should assign an array ref field (and other fields) to a mixed array of both persisted and to-be-persisted Schmea docs', (done) ->
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

    it 'should order the pkeys in an array ref field, not in the order of array ref member doc creation in a single oplog', (done) ->
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

    it 'should retrieve an [ObjectId] Array Ref as the configured logical schema relation: [Schema]', (done) ->
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

  describe 'Inverse Refs as an Array', ->
    beforeEach (done) -> mongo.flush done
    it 'should persist a relation that is the collection of docs that point to me via a Ref field in their schemas', (done) ->
      oplog = []
      tweetsAttrs = [
        { status: 'hasta' }
        { status: 'la' }
        { status: 'vista' }
        { status: 'baby' }
      ]
      User.create
        name: 'Brian'
        tweets: tweetsAttrs
      , (err, user) ->
        should.equal null, err
        userId = ObjectId.fromString user.get '_id'
        tweets = user.get 'tweets'
        tweetIds = (ObjectId.fromString tweet.get '_id' for tweet in tweets)
        remaining = 1 + tweetIds.length
        mongo.adapter.findOne 'users', _id: userId, {}, (err, json) ->
          Object.keys(json).length.should.equal 2
          json._id.should.not.be.undefined
          json.name.should.not.be.undefined
          should.equal undefined, json.tweets
          --remaining || done()
        for tweetId, i in tweetIds
          do (i) ->
            mongo.adapter.findOne 'tweets', _id: tweetId, {}, (err, json) ->
              json.status.should.equal tweetsAttrs[i].status
              json.author.should.eql userId
              --remaining || done()
      , oplog

    it 'should retrieve a relation that is the collection of docs that point to me via a Ref field in their schemas', (done) ->
      oplog = []
      tweetsAttrs = [
        { status: 'hasta' }
        { status: 'la' }
        { status: 'vista' }
        { status: 'baby' }
      ]
      User.create
        name: 'Brian'
        tweets: tweetsAttrs
      , (err, user) ->
        should.equal null, err
        userId = user.get '_id'
        User.findOne _id: userId, {select: ['_id', 'tweets']}, (err, foundUser) ->
          should.equal null, err
          tweets = foundUser.get 'tweets'
          for tweet, i in tweets
            tweet.should.be.an.instanceof Tweet
            tweet.get('status').should.equal tweetsAttrs[i].status
            tweet.get('author').should.equal foundUser
          done()
      , oplog

    it 'should auto-retrieve collections of docs that point (via a Ref field in their schemas) to the docs in the immediate result-set of a find query', (done) ->
      oplog = []
      tweetsAttrsA = [
        { status: 'hasta' }
        { status: 'la' }
        { status: 'vista' }
      ]
      tweetsAttrsB = [
        { status: 'baby' }
      ]
      toCreateRemaining = 2
      createCb = (err, user) ->
        return if --toCreateRemaining
        should.equal null, err
        userId = user.get '_id'
        User.find name: 'Brian', {select: ['_id', 'tweets']}, (err, users) ->
          should.equal null, err

          tweetsA = users[0].get 'tweets'
          for tweet, i in tweetsA
            tweet.should.be.an.instanceof Tweet
            tweet.get('status').should.equal tweetsAttrsA[i].status

          tweetsB = users[1].get 'tweets'
          for tweet, i in tweetsB
            tweet.should.be.an.instanceof Tweet
            tweet.get('status').should.equal tweetsAttrsB[i].status

          done()

      User.create
        name: 'Brian',
        tweets: tweetsAttrsA
      , createCb
      , oplog
      User.create
        name: 'Brian',
        tweets: tweetsAttrsB
      , createCb
      , oplog

  describe 'command building', ->
    it 'should create a new update $set command for a single set', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.set 'name', 'Brian'
      commandSet = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = commandSet.singleCommand
      {method, args} = cmd.compile()
      args.should.eql [
        'users'
        {_id: id}
        { $set: { name: 'Brian' } }
        { upsert: true, safe: true }
      ]

    it 'should add a 2nd set to an existing update $set command after a 1st set generates that command', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.set 'name', 'Brian'
      u.set 'age', 26
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $set: { name: 'Brian', age: 26 } }
        { upsert: true, safe: true }
      ]

    it 'should create a new $unset command for a single del', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.del 'name'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $unset: { name: 1 } }
        { upsert: true, safe: true }
      ]

    it "a 2nd sequential del on the same schema doc but different field should add the field to the existing update $unset command involving the first del's field", ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.del 'name'
      u.del 'age'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $unset: { name: 1, age: 1 } }
        { upsert: true, safe: true }
      ]

    it 'should create a new update $push command for a single push', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { tags: 'nodejs'} }
        { upsert: true, safe: true }
      ]

    it 'pushing multiple items with a single push should result in a $pushAll command', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs', 'sf'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $pushAll: { tags: ['nodejs', 'sf']} }
        { upsert: true, safe: true }
      ]

    it '2 pushes should result in a $pushAll', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs'
      u.push 'tags', 'sf'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas
      cmd = cmdSeq.singleCommand
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $pushAll: { tags: ['nodejs', 'sf']} }
        { upsert: true, safe: true }
      ]

    it 'a single item push on fieldA, followed by a single item push on field B should result in 2 $push commands', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs'
      u.push 'keywords', 'sf'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas

      cmdIds = Object.keys cmdSeq.commandsById
      cmdIds.should.have.length 2

      cmd = cmdSeq.commandsById[cmdIds[0]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { tags: 'nodejs'} }
        { upsert: true, safe: true }
      ]

      cmd = cmdSeq.commandsById[cmdIds[1]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { keywords: 'sf'} }
        { upsert: true, safe: true }
      ]

    it 'a single item push on field A, followed by a multi-item push on field V should result in 1 $push command and 1 $pushAll command', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs'
      u.push 'keywords', 'sf', 'socal'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas

      cmdIds = Object.keys cmdSeq.commandsById
      cmdIds.should.have.length 2

      cmd = cmdSeq.commandsById[cmdIds[0]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { tags: 'nodejs'} }
        { upsert: true, safe: true }
      ]

      cmd = cmdSeq.commandsById[cmdIds[1]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $pushAll: { keywords: ['sf', 'socal']} }
        { upsert: true, safe: true }
      ]

    it 'a multi iterm push on field A, followed by a single item push on field B should result in 1 $pushAll command and 1 $push command', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.push 'tags', 'nodejs', 'sf'
      u.push 'keywords', 'socal'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas

      cmdIds = Object.keys cmdSeq.commandsById
      cmdIds.should.have.length 2

      cmd = cmdSeq.commandsById[cmdIds[0]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $pushAll: { tags: ['nodejs', 'sf']} }
        { upsert: true, safe: true }
      ]

      cmd = cmdSeq.commandsById[cmdIds[1]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { keywords: 'socal'} }
        { upsert: true, safe: true }
      ]

    it 'a set on field A followed by a single item push on field B should result in 1 $set an 1 $push command', ->
      isNew = false
      id = ObjectId.generate().toHexString()
      u = new User _id: id, isNew
      u.set 'name', 'Brian'
      u.push 'keywords', 'socal'
      cmdSeq = CommandSequence.fromOplog u.oplog, Schema._schemas

      cmdIds = Object.keys cmdSeq.commandsById
      cmdIds.should.have.length 2

      cmd = cmdSeq.commandsById[cmdIds[0]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $set: { name: 'Brian' } }
        { upsert: true, safe: true }
      ]

      cmd = cmdSeq.commandsById[cmdIds[1]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: id}
        { $push: { keywords: 'socal'} }
        { upsert: true, safe: true }
      ]

    it 'an oplog involving 2 different conditions should result in 2 separate queries for oplog single-item push on doc matching conditions A, then single-item push on doc matching conditions B', ->
      sharedOplog = []
      isNew = false
      idA = ObjectId.generate().toHexString()
      idB = ObjectId.generate().toHexString()
      u1 = new User _id: idA, isNew, sharedOplog
      u2 = new User _id: idB, isNew, sharedOplog
      u1.push 'tags', 'nodejs'
      u2.push 'tags', 'sf'

      cmdSeq = CommandSequence.fromOplog sharedOplog, Schema._schemas

      cmdIds = Object.keys cmdSeq.commandsById
      cmdIds.length.should.equal 2

      cmd = cmdSeq.commandsById[cmdIds[0]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: idA}
        { $push: { tags: 'nodejs' } }
        { upsert: true, safe: true }
      ]

      cmd = cmdSeq.commandsById[cmdIds[1]]
      {method, args} = cmd.compile()
      method.should.equal 'update'
      args.should.eql [
        'users'
        {_id: idB}
        { $push: { tags: 'sf'} }
        { upsert: true, safe: true }
      ]

  # TODO Add tests for change of isNew true -> false
