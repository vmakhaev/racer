{DataSource, Schema} = racer = require '../../../racer'
types = require './types'
Command = require '../../Command'
DataSchema = require '../../Data/Schema'
DataQuery = require '../../Data/Query'
Promise = require '../../../Promise'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend
  _name: 'Mongo'

  # The layer of abstraction that deals with db-specific commands
  AdapterClass: require './Adapter'

  types: types

  # Deduces the correct type based on the right-hand-side vals of
  # field configuration in data schema definitions, e.g.,
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

  # Returns the parameters used to initialize a Virtual type instance
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
        # e.g., toEval = '@user._id'
        [_, targetPath] = toEval.split '.'
        conds[path] = (doc) -> doc[targetPath]
        fkey = path
        pkey = targetPath
      return switch query.queryMethod
        when 'findOne' then types.OneInverse
        when 'find' then {
          typeParams:
            translateSet: (cmd, cmdSeq, path, val, doc, dataField) ->
              dependencyCmd = cmdSeq.commandsByCid[doc.cid]
              cmds = (cmdSeq.commandsByCid[cid] for {cid} in val)
              cmdSeq.pipe dependencyCmd, cmds[0], (incomingCid, extraAttrs) ->
                fkey = dataField.fkey
                pkeyName = dataField.pkey
                fkeyVal = extraAttrs[pkeyName]
                for cmd in cmds
                  switch cmd.method
                    when 'insert' then cmd.val[fkey]      = fkeyVal
                    when 'update' then cmd.val.$set[fkey] = fkeyVal
                    else
                      throw new Error "Command method #{cmd.method} isn't supported in this context"
            uncast: (arr) ->
              return (DataSkema.uncast json for json in arr)
          fieldParams:
            ns: DataSkema.ns
            fkey: fkey
            pkey: pkey
            queryMethod: 'find'
            query: query
            querify: (conds) ->
              _query = @query.clone()
              _query._conditions = @conds conds
              return _query
            conds: (conds) ->
              _conds = @query._conditions
              console.log "$$$$$$$"
              console.log conds
              console.log _conds
              xfConds = {}
              xfConds[k] = _conds[k](conds) for k of _conds
              return xfConds
        }

      # TODO Deprecate the following code?
      type = Object.create type
      type._baseQuery = query
      type.get = (doc) ->
        _baseQuery: query
        get: -> # `this` is the document
          {_conditions: conds} = query = @_baseQuery.clone()
          conds[path] = conds[path](@)
          return query.fire()
      return type

  # TODO Better lang via MongoCommandBuilder.handle 'set', (...) -> ?
  # @param {CommandSequence} the current command sequence we're building
  # @param {Schema}     doc that generated the incoming op params
  # @param {DataField}  the data source field
  # @param {Object}     conds is the oplog op's conds
  # @param {String}     path is the oplog op's path
  # @param {Object}     val is the oplog op's val
  set: (cmdSeq, doc, dataField, conds, path, val) ->
    dataType = dataField.type

    # if dataType instanceof DataSchema && cid = val.cid
    if dataType.maybeDeferTranslateSet?(cmdSeq, doc, dataField, conds, path, val)
      return cmdSeq

    # If prior pending ops are expecting a document with cid
    if (cid = doc.cid) && (pending = cmdSeq.pendingByCid[cid])
      delete cmdSeq.pendingByCid[cid]
      (newVal = {})[path] = val
      for [method, pendingDoc, pendingDataField, pendingConds, pendingPath, pendingVal] in pending
        @[method] cmdSeq, pendingDoc, pendingDataField, pendingConds, pendingPath, newVal
      return cmdSeq

    {ns} = dataField

    matchingCmd = cmdSeq.findCommand ns, conds, isMatch = (cmd) ->
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

    # Virtual data types modify existing commands, so don't create a matchingCmd
    # TODO Make matchingCmd findOrCreate happen inside the dataType. Instead, here,
    #      command creation happens in 2 places - (1) here if not a Virtual type
    #      or (2) in Virtual type's translateSet.
    unless dataType._name == 'Virtual'
      unless matchingCmd
        if cid = conds.__cid__
          if pending = cmdSeq.pendingByCid[cid]
            for [pdoc, pfield, pconds, ppath, pval] in pending
              @set cmdSeq, pdoc, dataField, pconds, ppath + '.' + path, val
            # Don't `delete cmdSe.pendingByCid[cid]` because we want to keep
            # around `pending` for any future ops that are grounded in cid
            return cmdSeq

        matchingCmd        = new Command @, ns, conds, doc
        matchingCmd.val    = {}
        matchingCmd.method = if conds.__cid__ then 'insert' else 'update'

        cmdSeq.index    matchingCmd
        cmdSeq.position matchingCmd

    unless dataType.translateSet
      throw new Error "Data type '#{dataType._name}' does not have method `translateSet`"
    dataType.translateSet matchingCmd, cmdSeq, path, val, doc, dataField
    return cmdSeq

  del: (cmdSeq, doc, dataField, conds, path) ->
    {ns} = dataField

    matchingCmd = cmdSeq.findCommand ns, conds, isMatch = (cmd) ->
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

      cmdSeq.index    matchingCmd
      cmdSeq.position matchingCmd

      return cmdSeq

    switch matchingCmd.method
      when 'update'
        matchingCmd.val.$unset[path] = 1
      else
        throw new Error 'Unsupported'
    return cmdSeq

  push: (cmdSeq, doc, dataField, conds, path, values...) ->
    {ns, type: dataType} = dataField

    if dataType.memberType instanceof DataSchema && cid = values[0].cid
      for {cid} in values
        # Handle embedded arrays of docs
        pending = cmdSeq.pendingByCid[cid] ||= []
        op = ['push'].concat Array::slice.call(arguments, 1)
        pending.push op
      return cmdSeq

    values = dataField.cast values if dataField?.cast

    matchingCmd = cmdSeq.findCommand ns, conds, isMatch = (cmd) ->
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
      @push cmdSeq, doc, ns, dataType.memberType, conds, path, (val.toJSON() for val in values)
      return cmdSeq

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
      cmdSeq.index    matchingCmd
      cmdSeq.position matchingCmd
      return cmdSeq

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
    return cmdSeq

  pop: (ns, field, cmd, path, values..., ver) ->
    throw new Error 'Unimplemented'
