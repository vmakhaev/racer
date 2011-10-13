should = require 'should'
Mongo = require 'Schema/Mongo'
Schema = require 'Schema'

User = Schema.extend 'User', 'users',
  _id: Number
  name: String
  age: Number
  tags: [String]
  keywords: [String]

module.exports =
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
