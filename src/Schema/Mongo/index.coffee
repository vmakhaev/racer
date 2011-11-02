DataSource = require '../DataSource'
types = require './types'
CommandSet = require '../CommandSet'
Command = require '../Command'
Schema = require '../index'
DataSchema = require '../DataSchema'

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
            type = Object.create type
            type.isPkey = true
          else if 'function' == typeof type[flag]
            if Array.isArray arg
              type[flag] arg...
            else
              type[flag] arg
          else if type[flag] is undefined
            type[flag] = arg
          else
            throw new Error "Unsupported type descriptor flag #{flag}"
        return type
      if foreignDataField = descriptor.$pointsTo
        {source, path, type: ftype} = foreignDataField
        if ftype.isPkey # If this field is a ref
          concreteRefType = Object.create source.types.Ref
          concreteRefType.pkeyType = ftype
          concreteRefType.pkeyName = path
          concreteRefType.pointsToField = foreignDataField
          return concreteRefType
        else
          throw new Error 'Ensure that that you are pointing to a pkey'
        # if array ref
        # if inverse ref
        # if inverse array ref

    if Array.isArray descriptor
      arrayType = types['Array']
      concreteArrayType = Object.create arrayType
      concreteArrayType.memberType = @inferType descriptor[0]
      return concreteArrayType

    if descriptor instanceof DataSchema
      return descriptor

    if type = types[descriptor.name || descriptor._name]
      return type

    throw new Error "Unsupported type descriptor #{descriptor}"

  _assignToUnflattened: (assignTo, flattenedPath, val) ->
    parts = flattenedPath.split '.'
    curr = assignTo
    lastIndex = parts.length - 1
    for part, i in parts
      if i == lastIndex
        curr[part] = val
      else
        curr = curr[part] ||= {}
    return curr

  # TODO Better lang via MongoCommandBuilder.handle 'set', (...) -> ?
  # @param {CommandSet} the current command set we're building
  # @param {Schema} doc that generated the incoming op params
  # @param {DataField} the data source field
  # @param {Object} conds is the oplog op's conds
  # @param {String} path is the oplog op's path
  # @param {Object} val is the oplog op's val
  # TODO DRY
  set: (cmdSet, doc, dataField, conds, path, val) ->
    if dataField.type instanceof DataSchema && cid = val.cid
      # Handle embedded docs
      pending = cmdSet.pendingByCid[cid] ||= []
      op = ['set'].concat Array::slice.call(arguments, 1)
      pending.push op
      return cmdSet

    # If prior pending ops are expecting a document with cid
    if (cid = doc.cid) && (pending = cmdSet.pendingByCid[cid])
      delete cmdSet.pendingByCid[cid]
      newVal = {}
      newVal[path] = val
      for [method, pendingDoc, pendingDataField, pendingConds, pendingPath, pendingVal] in pending
        @[method] cmdSet, pendingDoc, pendingDataField, pendingConds, pendingPath, newVal
      return cmdSet

    {ns} = dataField

    matchingCmd = cmdSet.findCommand ns, conds, (cmd) ->
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
      matchingCmd = new Command @, ns, conds, doc
      matchingCmd.val = {}
      if conds.__cid__
        matchingCmd.method = 'insert'
      else
        matchingCmd.method = 'update'
      cmdSet.index matchingCmd
      cmdSet.position matchingCmd

    if dataField.type._name == 'Ref'
      {pkeyName} = dataField.type
      # TODO Change condition to val.isNew
      if cid = val.cid # If the doc we're linking to is new
        dependencyCmd = cmdSet.commandsByCid[cid]
        # cmdSet.pipe targetCommand.extraAttr(pkeyName), cmd.setAs(path)
        cmdSet.pipe dependencyCmd, matchingCmd, (extraAttrs) ->
          switch matchingCmd.method
            when 'insert'
              matchingCmd.val[path] = extraAttrs[pkeyName]
            when 'update'
              matchingCmd.val.$set[path] = pkeyVal
            else
              throw new Error "Command method #{matchingCmd.method} isn't supported in this context"
        return cmdSet
      if pkeyVal = val[pkeyName]
        @set cmdSet, doc, dataField, conds, path, pkeyVal
        return cmdSet

    if dataField.type._name == 'Array' && dataField.type.memberType._name == 'Ref'
      {pkeyName} = dataField.type.memberType
      existingPkeyIndices = []

      positionCb = (prevPosFulfilledVals...) ->
        pkeyVals = (pkeyVal for [extraAttrs] in prevPosFulfilledVals when pkeyVal = extraAttrs[pkeyName])
        # Add the pkeys of the docs we didn't have to create back into the array
        for [pkeyVal, i] in existingPkeyIndices
          pkeyVals.splice i, 0, pkeyVal
        switch matchingCmd.method
          when 'insert'
            matchingCmd.val[path] = pkeyVals
          when 'update'
            matchingCmd.val.$set[path] = pkeyVals
          else
            throw new Error "Command method #{matchingCmd.method} isn't supported in this context"
      
      if matchingCmd.pos
        positionMethod = null
      else
        positionMethod = 'position'
        positionArgs = [matchingCmd, positionCb]
      for mem, i in val
        if (mem.isNew) && !pos # If mem.cid, i.e., if the doc we're linking to is new
          positionMethod = 'placeAfterPosition'
          pos = cmdSet.commandsByCid[mem.cid].pos
          positionArgs = [pos, matchingCmd, null, positionCb]
        else if pkeyVal = dataField.type.memberType.cast mem.get pkeyName
          # TODO Next line does un-necessary work when there are no dependencies to create
          existingPkeyIndices.push [pkeyVal, i]
      
      if positionMethod
        cmdSet[positionMethod] positionArgs...
      else
        pkeyVals = (pkeyVal for [pkeyVal] in existingPkeyIndices)
        switch matchingCmd.method
          when 'update'
            set = matchingCmd.val.$set ||= {}
            set[path] = pkeyVals
          when 'insert'
            if -1 == path.indexOf '.'
              matchingCmd.val[path] = pkeyVals
            else
              @_assignToUnflattened matchingCmd.val, path, pkeyVals
      return cmdSet

    val = dataField.cast val if dataField.cast

    switch matchingCmd.method
      when 'update'
        set = matchingCmd.val.$set ||= {}
        set[path] = val
      when 'insert'
        if -1 == path.indexOf '.'
          matchingCmd.val[path] = val
        else
          @_assignToUnflattened matchingCmd.val, path, val
      else
        throw new Error 'Implement for other incoming method ' + matchingCmd.method

    return cmdSet


  del: (cmdSet, doc, dataField, conds, path) ->
    {ns} = dataField

    matchingCmd = cmdSet.findCommand ns, conds, (cmd) ->
      if cmd.method == 'update'
        $atomics = Object.keys cmd.val
        if $atomics.length > 1
          throw new Error 'Should not have > 1 $atomic per command'
        return true if '$unset' of cmd.val
      return false

    unless matchingCmd
      matchingCmd = new Command @, ns, conds, doc
      matchingCmd.method = 'update'
      (unset = {})[path] = 1
      matchingCmd.val = $unset: unset

      cmdSet.index matchingCmd
      cmdSet.position matchingCmd

      return cmdSet

    switch matchingCmd.method
      when 'update'
        matchingCmd.val.$unset[path] = 1
      else
        throw new Error 'Unsupported'
    return cmdSet

  push: (cmdSet, doc, dataField, conds, path, values...) ->
    {ns} = dataField

    if dataField.type.memberType instanceof DataSchema && cid = values[0].cid
      for {cid} in values
        # Handle embedded arrays of docs
        pending = cmdSet.pendingByCid[cid] ||= []
        op = ['push'].concat Array::slice.call(arguments, 1)
        pending.push op
      return cmdSet

    values = dataField.cast values if dataField?.cast

    matchingCmd = cmdSet.findCommand ns, conds, (cmd) ->
      {method: cmethod, val: cval} = cmd
      switch cmethod
        when 'insert' then return true
        when 'update'
          $atomics = Object.keys cmd.val
          if $atomics.length > 1
            throw new Error 'Should not have > 1 $atomic per command'
          if pushParam = cval.$push || cval.$pushAll
            return true if path of pushParam

    if dataField.type.memberType._name == 'Object' && values[0] instanceof Schema
      memberField = dataField.type.memberType.createField()
      @push cmdSet, doc, ns, dataField.type.memberType, conds, path, (val._doc for val in values)
      return cmdSet

    unless matchingCmd
      matchingCmd = new Command @, ns, conds, doc
      if conds.__cid__
        matchingCmd.method = 'insert'
        (matchingCmd.val = {})[path] = values
      else
        matchingCmd.method = 'update'
        if values.length == 1
          val = values[0]
          k = '$push'
        else if values.length > 1
          val = values
          k = '$pushAll'
        else
          throw new Error 'length of 0! Uh oh!'
        (args = {})[path] = val
        (matchingCmd.val ||= {})[k] = args
      cmdSet.index matchingCmd
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
