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

exports.Array =
  name: 'Array'
  
  cast: (list) ->
    return (@memberType.cast member for member in list)

# Object means an embedded document or the member of an embedded 
# array if this is a recursive inferType call
# TODO Can we remove this?
exports.Object =
  name: 'Object'
