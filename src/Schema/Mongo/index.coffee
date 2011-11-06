DataSource = require '../DataSource'
types = require './types'
CommandSet = require '../CommandSet'
Command = require '../Command'
Schema = require '../index'
DataSchema = require '../DataSchema'
DataQuery = require '../DataQuery'
Promise = require '../../Promise'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend
  _name: 'Mongo'

  # The layer of abstraction that deals with db-specific commands
  AdapterClass: require './Adapter'

  types: types

  # Where the magical interpretation happens of the right-hand-side vals of fields in, e.g.,
  #     CustomSchema.source(source, ns, {
  #       fieldName: descriptorToInfer
  #       fieldB:    anotherDescriptor
  #     });
  inferType: (descriptor) ->
    if descriptor.constructor == Object
      if type = descriptor.$type
        # If we have { $type: SomeType, ... } as our Data Source Schema attribute rvalue
        delete descriptor.$type
        for flag, arg of descriptor
          if flag == '$pkey'
            type = Object.create type, isPkey: value: true
            continue
          if typeof type[flag] is 'function'
            if Array.isArray arg
              type[flag] arg...
            else
              type[flag] arg
            continue
          if type[flag] isnt undefined
            throw new Error "Unsupported type descriptor flag #{flag}"
          type[flag] = arg
        return type
      if foreignDataField = descriptor.$pointsTo
        {source, path, type: ftype} = foreignDataField
        if ftype.isPkey # If this field is a ref
          return Object.create source.types.Ref,
            pkeyType:      { value: ftype }
            pkeyName:      { value: path  }
            pointsToField: { value: foreignDataField }
        else
          throw new Error 'Ensure that that you are pointing to a pkey'
        # if array ref
        # if inverse ref
        # if inverse array ref

    if Array.isArray descriptor
      return Object.create types.Array,
        memberType: value: @inferType descriptor[0]

    if descriptor instanceof DataSchema
      return descriptor

    if type = types[descriptor.name || descriptor._name]
      return type

    throw new Error "Unsupported type descriptor #{descriptor}"

  # Returns the parameters used to initialize a Virtual type instances
  # based on the meaning of the descriptor.
  virtualParams: (descriptor) ->
    # Descriptor for inverse of Ref or [Ref]
    # User.createDataSchema(mongo, {
    #   profiles: mongo.Profile.find().where('userId', '@user.id')
    # });
    if descriptor instanceof DataQuery
      query = descriptor
      DataSkema = query.schema
      conds = query._conditions
      for path, toEval of conds
        [_, targetPath] = toEval.split '.'
        conds[path] = (doc) -> doc[targetPath]
#      type = switch query.queryMethod
      return switch query.queryMethod
        when 'findOne' then types.OneInverse
#        when 'find'    then types.ManyInverse
        when 'find' then {
          typeParams:
            shouldIgnoreSet: true
          fieldParams:
            ns: DataSkema.ns
        }
      type = Object.create type
      type._baseQuery = query
      type.get = (doc) ->
        _baseQuery: query
        get: -> # `this` is the document
          {_conditions: conds} = query = @_baseQuery.clone()
          conds[path] = conds[path](@)
          return query.fire()
      return type

  # Takes flattenedPath and traverses the object, assignTo, to the corresponding
  # node. Then, assigns val to this node.
  # @param {Object} assignTo
  # @param {String} flattenedPath
  # @param {Object} val
  _assignToUnflattened: (assignTo, flattenedPath, val) ->
    curr      = assignTo
    parts     = flattenedPath.split '.'
    lastIndex = parts.length - 1
    for part, i in parts
      if i == lastIndex
        curr[part] = val
      else
        curr = curr[part] ||= {}
    return curr

  # TODO Better lang via MongoCommandBuilder.handle 'set', (...) -> ?
  # @param {CommandSet} the current command set we're building
  # @param {Schema}     doc that generated the incoming op params
  # @param {DataField}  the data source field
  # @param {Object}     conds is the oplog op's conds
  # @param {String}     path is the oplog op's path
  # @param {Object}     val is the oplog op's val
  set: (cmdSet, doc, dataField, conds, path, val) ->
    dataType = dataField.type

    return cmdSet if dataType.shouldIgnoreSet

    # if dataType instanceof DataSchema && cid = val.cid
    if didDefer = dataType.maybeDeferTranslateSet?(cmdSet, doc, dataField, conds, path, val)
      return cmdSet

    # If prior pending ops are expecting a document with cid
    if (cid = doc.cid) && (pending = cmdSet.pendingByCid[cid])
      delete cmdSet.pendingByCid[cid]
      (newVal = {})[path] = val
      for [method, pendingDoc, pendingDataField, pendingConds, pendingPath, pendingVal] in pending
        @[method] cmdSet, pendingDoc, pendingDataField, pendingConds, pendingPath, newVal
      return cmdSet

    {ns} = dataField

    matchingCmd = cmdSet.findCommand ns, conds, isMatch = (cmd) ->
      {method: cmethod, val: cval} = cmd
      switch cmethod
        when 'insert' then return true
        when 'update'
          $atomics = Object.keys cval
          if $atomics.length > 1
            throw new Error 'Should not have > 1 $atomic per command'
          if '$set' of cval
            return true
      return false

    unless matchingCmd
      if cid = conds.__cid__
        if pending = cmdSet.pendingByCid[cid]
          for [pdoc, pfield, pconds, ppath, pval] in pending
            @set cmdSet, pdoc, dataField, pconds, ppath + '.' + path, val
          # We want to keep around pending for any future ops that are grounded in cid
          # , so we don't delete cmdSet.pendingByCid[cid]
          return cmdSet

      matchingCmd        = new Command @, ns, conds, doc
      matchingCmd.val    = {}
      matchingCmd.method = if conds.__cid__ then 'insert' else 'update'

      cmdSet.index    matchingCmd
      cmdSet.position matchingCmd

    unless dataType.translateSet
      throw new Error "Data type '#{dataType._name}' does not have method `translateSet`"
    dataType.translateSet matchingCmd, cmdSet, path, val
    return cmdSet

  del: (cmdSet, doc, dataField, conds, path) ->
    {ns} = dataField

    matchingCmd = cmdSet.findCommand ns, conds, isMatch = (cmd) ->
      if cmd.method == 'update'
        $atomics = Object.keys cmd.val
        if $atomics.length > 1
          throw new Error 'Should not have > 1 $atomic per command'
        return true if '$unset' of cmd.val
      return false

    unless matchingCmd
      (unset = {})[path] = 1
      matchingCmd        = new Command @, ns, conds, doc
      matchingCmd.method = 'update'
      matchingCmd.val    = $unset: unset

      cmdSet.index    matchingCmd
      cmdSet.position matchingCmd

      return cmdSet

    switch matchingCmd.method
      when 'update'
        matchingCmd.val.$unset[path] = 1
      else
        throw new Error 'Unsupported'
    return cmdSet

  push: (cmdSet, doc, dataField, conds, path, values...) ->
    {ns, type: dataType} = dataField

    if dataType.memberType instanceof DataSchema && cid = values[0].cid
      for {cid} in values
        # Handle embedded arrays of docs
        pending = cmdSet.pendingByCid[cid] ||= []
        op = ['push'].concat Array::slice.call(arguments, 1)
        pending.push op
      return cmdSet

    values = dataField.cast values if dataField?.cast

    matchingCmd = cmdSet.findCommand ns, conds, isMatch = (cmd) ->
      {method: cmethod, val: cval} = cmd
      switch cmethod
        when 'insert' then return true
        when 'update'
          $atomics = Object.keys cmd.val
          if $atomics.length > 1
            throw new Error 'Should not have > 1 $atomic per command'
          if pushParam = cval.$push || cval.$pushAll
            return true if path of pushParam

    if dataType.memberType._name == 'Object' && values[0] instanceof Schema
      memberField = dataType.memberType.createField()
      @push cmdSet, doc, ns, dataType.memberType, conds, path, (val._doc for val in values)
      return cmdSet

    unless matchingCmd
      matchingCmd = new Command @, ns, conds, doc
      if conds.__cid__
        matchingCmd.method = 'insert'
        (matchingCmd.val = {})[path] = values
      else
        matchingCmd.method = 'update'
        len = values.length
        if      len == 1 then val = values[0]; k = '$push'
        else if len > 1  then val = values   ; k = '$pushAll'
        else
          throw new Error 'length of 0! Uh oh!'
        (args = {})[path] = val
        (matchingCmd.val ||= {})[k] = args
      cmdSet.index    matchingCmd
      cmdSet.position matchingCmd
      return cmdSet

    switch matchingCmd.method
      when 'update'
        if existingPush = matchingCmd.val.$push[path]
          matchingCmd.val.$pushAll = {}
          matchingCmd.val.$pushAll[path] = [existingPush, values...]
          delete matchingCmd.val.$push
        else if existingPush = matchingCmd.val.$pushAll[path]
          existingPath.push values...
        else
          throw new Error 'matchingCmd should house either push or pushAll'
      when 'insert'
        arr = matchingCmd.val[path] ||= []
        arr.push values...
      else
        throw new Error 'Unimplemented'
    return cmdSet

  pop: (ns, field, cmd, path, values..., ver) ->
    throw new Error 'Unimplemented'
