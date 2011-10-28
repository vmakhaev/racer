Schema = require '../index'
{merge} = require '../../util'

baseType =
  createField: -> return Object.create @
  extend: (name, conf) ->
    extType = Object.create @
    extType._name = name
    merge extType, conf
    return extType

NativeObjectId = require('mongodb').BSONPure.ObjectID
exports.ObjectId = baseType.extend 'ObjectId',
  cast: (val) ->
    return val if val instanceof NativeObjectId
    return @fromString val

  uncast: (oid) ->
    return oid.toString()

  defaultTo: -> new NativeObjectId

  fromString: (str) ->
    unless 'string' == typeof str && 24 == str.length
      throw new Error 'Invalid ObjectId'
    return NativeObjectId.createFromHexString str

  toString: (oid) ->
    return NativeObjectId.toString() unless arguments.length
    return oid.toHexString()

exports.Array = baseType.extend 'Array',
  cast: (list) ->
    # Returns an array comprehension
    memberType = @memberType
    for member in list
      if memberType.cast
        memberType.cast member
      else
        member

  createField: ({memberType}) ->
    field = Object.create @
    field.memberType = memberType
    return field

# Object means an embedded document or the member of an embedded 
# array if this is a recursive inferType call
# TODO Can we remove exports.Object type?
exports.Object = baseType.extend 'Object',
  cast: (val) ->
    return val.toJSON() if val instanceof Schema
    return val

exports.Ref = baseType.extend 'Ref',
  cast: (val) ->
    return @pkeyType.cast val

  createField: ({pkeyType, pkeyName, source}) ->
    field = Object.create @
    field.pkeyType = pkeyType
    field.pkeyName = pkeyName
    field.source = source
    return field

  deref: (pkeyVal, callback) ->
    @source.findOne 
