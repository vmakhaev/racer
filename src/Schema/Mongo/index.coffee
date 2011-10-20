DataSource = require '../DataSource'
types = require './types'
CommandSet = require '../CommandSet'
Command = require '../Command'
Schema = require '../index'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend

  _name: 'Mongo'

  # The layer of abstraction that deals with db-specific commands
  AdapterClass: require './Adapter'

  types: types

  # Where the magical interpretation happens of the right-hand-side vals
  # of attributes in config of
  #     CustomSchema.source source, ns, config
  inferType: (descriptor) ->
    if descriptor.constructor == Object
      if '$type' of descriptor
        # If we have { $type: SomeType, ... } as our Data Source Schema attribute rvalue
        type = @inferType descriptor.$type
        delete descriptor.$type
        for flag, arg of descriptor
          if 'function' == typeof type[flag]
            if Array.isArray arg
              type[flag] arg...
            else
              type[flag] arg
          else if type[flag] is undefined
            type[flag] = arg
          else if flag == '$pkey'
          else
            throw new Error "Unsupported type descriptor flag #{flag}"
        return type
      if '$dataField' of descriptor
        {source, fieldName, type: ftype} = descriptor.$dataField
        if ftype.$pkey # If this field is a ref
          # source.inferType 
          return source.types.Ref.createField
            pkeyType: ftype
            pkeyName: fieldName
            source: source
        # if array ref
        # if inverse ref
        # if inverse array ref

    if Array.isArray descriptor
      arrayType = types['Array']
      memberType = descriptor[0]
      concreteArrayType = Object.create arrayType
      concreteArrayType.memberType = @inferType memberType
      return concreteArrayType

    if type = types[descriptor.name || descriptor._name]
      return type

    # else String, Number, Object => Take things as they are
    return {}

  _oplogToCommandSet: (oplog, cmdSet = new CommandSet) ->
    for op in oplog
      {doc, ns, conds, method, path, args} = operation.splat op

      # TODO Handle nested paths

      # Filter out paths that don't map to this Data Source
      continue unless field = @fields[ns][path]
      @[method] cmdSet, doc, ns, field, conds, args...
    return cmdSet

  # e.g., accomplish optimizations such as collapsing
  # multiple sequental push ops into a single atomic push
  _minifyOps: (oplog) -> oplog

  # TODO Better lang via MongoCommandBuilder.handle 'set', (...) -> ?
  # @param {CommandSet} is the current command set that we are building
  # @param {String} ns is the namespace
  # @param {Object} field is the data source field
  # @param {Object} conds is the oplog op's conds
  # @param {String} path is the oplog op's path
  # @param {Object} val is the oplog op's val
  # @param {Number} ver is the oplog op's ver
  set: (cmdSet, doc, ns, field, conds, path, val, ver) ->
    cmds = cmdSet.findCommands ns, conds

    for cmd in cmds
      cmethod = cmd.method
      if cmethod == 'insert'
        matchingCmd = cmd
        break
      else if cmethod == 'update'
        $atomics = Object.keys cmd.val
        if $atomics.length > 1
          throw new Error 'Should not have > 1 $atomic per command'
        if '$set' of $atomics
          matchingCmd = cmd
          break
    
    if field._name == 'Ref'
      {pkeyName} = field
      if cid = val.cid # If the doc we're linking to is new
        dependencyCmd = cmdSet.findCommandByCid cid
        # cmdSet.pipe targetCommand.extraAttr(pkeyName), cmd.setAs(path)
        cmdSet.pipe dependencyCmd, cmd, (extraAttrs) =>
          if matchingCmd.method == 'insert'
            matchingCmd.val[path] = extraAttrs[pkeyName]
          else if matchingCmd.method == 'update'
            matchingCmd.val.$set[path] = pkeyVal
          else
            throw new Error "command method  #{matchingCmd.method} is not supported in this context"
        return cmdSet
      if pkeyVal = val[pkeyName]
        @set cmdSet, ns, field.type, conds, path, pkeyVal, ver
        return cmdSet

    unless matchingCmd
      matchingCmd = new Command ns, conds, doc
      if conds.__cid__
        matchingCmd.method = 'insert'
        (matchingCmd.val = {})[path] = val
      else
        matchingCmd.method = 'update'
        (delta = {})[path] = value
        matchingCmd.val = { $set: delta}

      cmdSet.index matchingCmd
      cmdSet.position matchingCmd
      return cmdSet

    val = field.cast val if field.cast

    switch matchingCmd.method
      when 'update'
        cmd.val.$set[path] = val
      when 'insert'
        cmd.val[path] = val
      else
        throw new Error 'Implement for other incoming method ' + matchingCmd.method

    return cmdSet

  del: (ns, field, cmd, conds, path) ->
    # Assign or augment cmd.(method|conds|val)
    {method: cmethod, conds: cconds} = cmd
    if cmethod is undefined && cconds is undefined
      cmd.ns = ns
      cmd.method = 'update'
      cmd.conds = conds
      (unset = {})[path] = 1
      cmd.val = { $unset: unset }
    else if cmethod == 'update'
      if (unset = cmd.val.$unset) && objEquiv cconds, conds
        unset[path] = 1
      else
        # Either the existing cmd involves another $atomic, or the
        # conditions of the existing cmd do not match the incoming
        # op conditions. In both cases, we must create a new cmd
        nextCommand = {}
        [nextCommand] = @del ns, field, nextCommand, conds, path, val, ver
    else
      # The current cmd involves
      nextCommand = {}
      [nextCommand] = @del ns, field, nextCommand, conds, path, val, ver
    return [cmd, nextCommand]

  push: (cmdSet, doc, ns, field, conds, path, values...) ->
    values = field.cast values if field.cast

    if field.memberType._name == 'Object' && values[0] instanceof Schema
      @push cmdSet, doc, ns, field.memberType, conds, path, (val._doc for val in values)
      return cmdSet

    cmds = cmdSet.findCommands ns, conds

    for cmd in cmds
      cmethod = cmd.method
      if cmethod == 'insert'
        matchingCmd = cmd
        break
      else if cmethod == 'update'
        $atomics = Object.keys cmd.val
        if $atomics.length > 1
          throw new Error 'Should not have > 1 $atomic per command'
        if ('$push' of $atomics) || ('$pushAll' of $atomics)
          matchingCmd = cmd
          break

    unless matchingCmd
      matchingCmd = new Command ns, conds, doc
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
        if existingPush = matchingCmd.$push[path]
          matchingCmd.val.$pushAll = {}
          matchingCmd.val.$pushAll[path] = [existingPush, values...]
          delete matchingCmd.val.$push
        else if existingPush = matchingCmd.val.$pushAll[path]
          existingPath.push values...
        else
          throw new Error 'matchingCmd should house either push or pushAll'
      when 'insert'
        arr = cmd.val[path] ||= []
        arr.push values...
      else
        throw new Error 'Unimplemented'
    return cmdSet

  pop: (ns, field, cmd, path, values..., ver) ->
    throw new Error 'Unimplemented'

operation =
  splat: ([doc, namespace, conds, method, args...]) ->
    {doc, ns: namespace, conds, method, path: args[0], args}
