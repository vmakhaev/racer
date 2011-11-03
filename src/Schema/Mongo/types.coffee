Schema = require '../index'
{merge} = require '../../util'
DataField = require '../DataField'
Promise = require '../../Promise'

baseType =
  createField: (opts) -> new DataField @, opts

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
    return oid.toHexString()

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

  # TODO Do we need uncast for Array?
  uncast: (arr) ->
    memberType = @memberType
    for v in arr
      if memberType.uncast
        memberType.uncast v
      else
        v

  createField: (opts) ->
    field = new DataField @, opts
    if @memberType._name == 'Ref'
      # @param {[ObjectId]} pkeyVals
      # @param {Function} callback(err, arrOfDereffedJson)
      field.deref = (pkeyVals, callback) ->
        # TODO DRY up this and Ref.createField's field.deref
        derefProm = new Promise
        derefProm.bothback callback
        {pkeyName}  = memberType = @type.memberType
        {source, ns} = memberType.pointsToField
        conds = {}
        DataSkema = source.dataSchemasWithNs[ns]
        remaining = pkeyVals.length
        arr = []
        for pkeyVal, i in pkeyVals
          conds[pkeyName] = pkeyVal
          do (i) ->
            DataSkema.findOne conds, null, (err, json) ->
              return derefProm.fail err if err
              arr[i] = json
              --remaining || derefProm.fulfill arr
        return derefProm
    return field

exports.Ref = baseType.extend 'Ref',
  cast: (val) ->
    return @pkeyType.cast val

  createField: (opts) ->
    field = new DataField @, opts
    field.deref = (pkeyVal, callback) ->
      {source, ns} = @type.pointsToField
      conds = {}
      conds[@type.pkeyName] = pkeyVal
      # Change null to explicit fields
      return source.dataSchemasWithNs[ns].findOne conds, null, callback
    return field

for type in ['String', 'Number']
  exports[type] = baseType.extend type, {}
