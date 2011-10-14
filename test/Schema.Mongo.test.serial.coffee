should = require 'should'
Mongo = require 'Schema/Mongo'
{schema} = Schema = require 'Schema'
schema = -> {}

schema = -> {}
ObjectId = String

# Blog = Schema.extend 'Blog', 'blogs',
#   _id: String
#   name: String

User = Schema.extend 'User', 'users',
  _id: String
  name: String
  age: Number
  tags: [String]
  keywords: [String]
  luckyNumbers: [Number]
#  blog: Blog
#  friends: [schema('User')]
  # BRIAN: A Schema should be a special Type?
  # But Types only make sense in the context of being used
  # in a Schema definition. So perhaps, it is more accurate
  # to say that Schema can have a Type interface
#  group: schema('Group')

User.source Mongo, 'los_users',
  _id: ObjectId
  name: String
  age: Number
  tags: [String]
  keywords: [String]
  friendIds: [User._id]
  groupId: schema(Group)._id
,
  friends: User.friendIds
#   # Alt A
#   friends: User.where('_id').findOne (user) ->
#     User.where('_id', user.friendIds).find()
#   # Alt B
#   friends: (id) -> User.where('_id', User.where('_id', id).findOne().friendIds).find()

Group = Schema.extend 'Group', 'groups',
  _id: String
  name: String
#  users: [User]

Group.source Mongo,
  _id: ObjectId
  name: String
,
  users: [User.groupId]
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
# User.source Mongo, 'los_users',
#   _id: Mongo.pkey
#   # ...
#   friendIds: [User._id]
#   blogId: Blog._id
# ,
#   friends: friendIds
# 
# # Scen A - array of refs
# Blog.source Mongo,
#   _id: ObjectId
#   authorIds: [User._id]
# ,
#   authors: 'authorIds'
# 
# User.source Mongo,
#   _id: ObjectId
# ,
#   blog: pointedToBy(Blog.authors)
# 
# # Scen B - ref
# Blog.source Mongo,
#   _id: ObjectId
# User.source Mongo,
#   _id: ObjectId
#   blogId: Blog._id
# ,
#   blog: 'blogId'
# 
# # Scen C - Inverse ref
# Blog.source Mongo,
#   _id: ObjectId
#   authorId: schema('User')._id # (*)
# 
# User.source Mongo,
#   _id: ObjectId
# ,
#   blog: Blog.authorId

module.exports =
  # Casting
  'logical schema layer should cast String attributes': (done) ->
    s = new User _id: 1
    s.get('_id').should.equal '1'
    done()

  'logical schema layer should cast Number attributes': (done) ->
    s = new User age: '26'
    s.get('age').should.equal 26
    done()

  'logical schema layer should cast [String] attributes': (done) ->
    u = new User tags: [1, 2, 3, 4]
    u.get('tags').should.eql ['1', '2', '3', '4']
    done()

  'logical schema layer should cast [Number] attributes': (done) ->
    s = new User luckyNumbers: ['4', 8, '12', 16]
    s.get('luckyNumbers').should.eql [4, 8, 12, 16]
    done()

  # Query building
  'should create a new update $set query for a single set': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.set 'name', 'Brian'
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $set: { name: 'Brian', age: 26 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new $unset query for a single del': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.del 'name'
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $unset: { name: 1, age: 1 } }
      { upsert: true, safe: true }
    ]
    done()

  'should create a new update $push query for a single push': (done) ->
    addOp = false
    s = new User _id: 1, addOp
    s.push 'tags', 'nodejs'
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 1
    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $push: { tags: 'nodejs'} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $pushAll: { tags: ['nodejs', 'sf']} }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
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
    m = new Mongo
    queries = m._queriesForOps s.oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $set: { name: 'Brian' } }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
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

    m = new Mongo
    queries = m._queriesForOps oplog
    queries.length.should.equal 2

    {method, args} = queries[0]
    method.should.equal 'update'
    args.should.eql [
      {_id: 1}
      { $push: { tags: 'nodejs' } }
      { upsert: true, safe: true }
    ]

    {method, args} = queries[1]
    method.should.equal 'update'
    args.should.eql [
      {_id: 2}
      { $push: { tags: 'sf'} }
      { upsert: true, safe: true }
    ]

    done()
