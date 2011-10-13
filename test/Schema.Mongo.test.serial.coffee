should = require 'should'
Mongo = require 'Schema/Mongo'
Schema = require 'Schema'

User = Schema.extend 'User', 'users',
  _id: Number
  name: String
  age: Number
  tags: [String]

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
