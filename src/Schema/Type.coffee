# We could have done style of:
#     NumberType = Type.extend 'Number,
#       cast: (val) ->
#
# where NumberType has constructor signature:
#     function NumberType (path, opts) { /**/ }
#
# but then we must deal with issues around guaranteeing
# for
#     num = new NumberType
#     num.should.be.an.instanceof Number
#     typeof(num).should.equal 'number'
#
# The same is even more important for Array type derivates,
# since "sub-classing" Arrays in JS is full of anomalies.
#
# # Casting
#
# We will want to use casting on both field assignment & mutation
# and query parameters.
#
# # Who calls the field methods?
#     # For numbers and "primitive types
#     user.get('age').increment(2)
#     # vs
#     user.increment('age', 2)
#
#     # For complex types like Schemas that have their own methods
#     user.get('group').set('name', 'nodejs')
#
#     # For arrays, use native Array
#     friends = user.get('friends')
#     friends.length
#     friends[i]
#     friends.push
#
#     # For sets
#     friends = user.get('friends')
#     friends.add(new User)
#     friends.remove(_id: 5)
#
# This implies the need for 2 types:
# 1. Types that add methods to Schema to interact with that type.
#    Schema::[typeMethod] ||= (fieldName, args...) ->
#      currFieldVal = @_doc[fieldName]
#      @_doc[fieldName] = @_fields[fieldName][typeMethod] currFieldVal, args...
#
#      # @_fields[fieldName][typeMethod].apply currFieldVal, args...
#      # @_fields[fieldName][typeMethod].apply @_doc, args...
#      
# 2. Types whose methods are used directly via the type object.
#
# # Fields/Attributes vs Types
# Types are indexed by string name via Type or Schema. They encapsulate code
# for basic casting and validation.
#
# Fields are when you associate a type with a field. It is a materialization of
# that type on a Schema attribute. This instance should be able to add its own
# validations in addition to inheriting the validations of the Type.
#
# # Array types and collection types
# These types are interesting in that they are similar to Haskell type variables
# or C templates.
#     Set<T>
#     [t]
#
#     # Maybe the following syntax
#     TypeClass.define 'Set', (type) ->
#

Field = require './Field'

Type = module.exports = (name, config) ->
  @setups = []
  @validators = []

  for method, arg of config
    if method == 'cast'
      @cast = arg
      continue
    if Array.isArray arg
      @[method] arg...
    else
      @[method] arg
  return

Type:: =
  # TODO Rename this
  createField: -> new Field @
  assignAsTypeToSchemaField: (schema, fieldName) ->
    setups = @setups
    setup.call schema, fieldName for setup in setups
    schema[fieldName] = @

  setup: (fn) ->
    @setups.push fn
    return @

  caster: (caster) ->
    @cast = caster
    return @

  validator: (validator) ->
    @validators.push validator
    return @

  extend: (parentType) -> # TODO

  validate: (val) ->
    (fn val for fn in @validators)
