Schema = require '../../Logical/Schema'
{merge} = require '../../../util'
{assignToUnflattened} = require '../../../util/path'
DataField = require '../../Data/Field'
Promise = require '../../../Promise'
DataSchema = require '../../Data/Schema'
{baseType} = DataSchema::types

NativeObjectId = require('mongodb').BSONPure.ObjectID
exports.ObjectId = baseType.extend 'ObjectId',
  cast: (val) ->
    return val if val instanceof NativeObjectId
    return @fromString val

  uncast: (oid) ->
    return oid.toHexString()

  defaultTo: -> new NativeObjectId

  fromString: (str) ->
    unless typeof str is 'string' && 24 == str.length
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

  translateSet: (cmd, cmdSet, path, val) ->
    {pkeyName} = @memberType

    positionCb = (prevPosFulfilledVals...) ->
      pkeyVals = (pkeyVal for [cid, extraAttrs] in prevPosFulfilledVals when pkeyVal = extraAttrs[pkeyName])
      # Re-order prevPosFulfilledVals to match order of the array ref's doc ordering
      pkeyVals = []
      extraAttrsByCid = {}
      for [cid, extraAttrs] in prevPosFulfilledVals
        extraAttrsByCid[cid] = extraAttrs
      for {cid}, i in val
        if pkeyVal = extraAttrsByCid[cid]?[pkeyName]
          # Add the pkeys created by just-run insert commands
          pkeyVals[i] = pkeyVal
        else
          # Add the pkeys of the docs we didn't have to create
          [pkeyVal, j] = existingPkeyIndices.shift()
          if i != j
            throw new Error "Expected an existing doc's pkey at index #{i}, but the next existing doc was remembered at position #{j}"
          pkeyVals[j] = pkeyVal

      switch cmd.method
        when 'insert' then cmd.val[path]      = pkeyVals
        when 'update' then cmd.val.$set[path] = pkeyVals
        else
          throw new Error "Command method #{cmd.method} isn't supported in this context"

    if cmd.pos
      positionMethod = null
    else
      positionArgs   = [cmd, positionCb]
      positionMethod = 'position'
    existingPkeyIndices = []
    for mem, i in val
      if mem.isNew # If mem.cid, i.e., if the doc we're linking to is new
        unless pos
          pos            = cmdSet.commandsByCid[mem.cid].pos
          positionArgs   = [pos, cmd, null, positionCb]
          positionMethod = 'placeAfterPosition'
      else if pkeyVal = @memberType.cast mem.get pkeyName
        # TODO Next line does un-necessary work when there are no dependencies to create
        existingPkeyIndices.push [pkeyVal, i]

    if positionMethod
      cmdSet[positionMethod] positionArgs...
    else
      pkeyVals = (pkeyVal for [pkeyVal] in existingPkeyIndices)
      switch cmd.method
        when 'update'
          set = cmd.val.$set ||= {}
          set[path] = pkeyVals
        when 'insert'
          if -1 == path.indexOf '.'
            cmd.val[path] = pkeyVals
          else
            assignToUnflattened cmd.val, path, pkeyVals
    return true

exports.Ref = baseType.extend 'Ref',
  cast: (val) ->
    return @pkeyType.cast val

  uncast: (val) ->
    # TODO Fix abstractions. uncast here works differently depending on the incoming val type
    return val if val.constructor == Object
    console.trace()
    return @pkeyType.uncast val

  createField: (opts) ->
    field = new DataField @, opts
    field.deref = (pkeyVal, callback) ->
      {source, ns} = @type.pointsToField
      conds = {}
      conds[@type.pkeyName] = pkeyVal
      # Change null to explicit fields
      return source.dataSchemasWithNs[ns].findOne conds, null, callback
    return field

  translateSet: (cmd, cmdSet, path, val) ->
    pkeyName = @pkeyName
    if ((val instanceof Schema && val.isNew) || !(val instanceof Schema)) && cid = val.cid
      dependencyCmd = cmdSet.commandsByCid[cid]
      # cmdSet.pipe targetCommand.extraAttr(pkeyName), cmd.setAs(path)
      cmdSet.pipe dependencyCmd, cmd, (incomingCid, extraAttrs) ->
        if incomingCid != cid
          throw new Error "Calling back with extraAttrs specified for doc cid #{cid}
            when we are expecting extraAttrs specified for command associated with
            doc cid #{incomingCid}"
        pkeyVal = extraAttrs[pkeyName]
        switch cmd.method
          when 'insert' then cmd.val[path]      = pkeyVal
          when 'update' then cmd.val.$set[path] = pkeyVal
          else
            throw new Error "Command method #{cmd.method} isn't supported in this context"
      return true
    if pkeyVal = val[pkeyName]
      # TODO What if pkey is not supposed to be an ObjectId
      exports.ObjectId.translateSet cmd, cmdSet, path, pkeyVal
      return true
    return false

exports.Virtual = baseType.extend 'Virtual', {}

for type in ['String', 'Number']
  exports[type] = baseType.extend type, {}
