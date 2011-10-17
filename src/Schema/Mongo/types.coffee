NativeObjectId = require('mongodb').BSONPure.ObjectID
exports.ObjectId =
  name: 'ObjectId'
  NativeObjectId: NativeObjectId

  cast: (val) ->
    return val if val instanceof NativeObjectId
    return @fromString val

  defaultTo: -> new NativeObjectId

  fromString: (str) ->
    unless 'string' == typeof str && 24 == str.length
      throw new Error 'Invalid ObjectId'
    return NativeObjectId.createFromHexString str

  toString: (oid) ->
    return NativeObjectId.toString() unless arguments.length
    return oid.toHexString()
