{objEquiv} = require '../../util'
DataSource = require '../DataSource'
types = require './types'
CommandSet = require './CommandSet'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend

  # The layer of abstraction that deals with db-specific commands
  AdapterClass: require './adapter'

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
          else
            throw new Error "Unsupported type descriptor flag #{flag}"
        return type
      if '$dataField' of descriptor
        {source, schema, fieldName, type: ftype} = descriptor.$ref
        if ftype.$pkey # If this field is a ref
          # source.inferType 
          return source.types.Ref.createField
            pkeyType: ftype.$type
            pkeyName: ftype.fieldName
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
      {ns, conds, method, path, args} = operation.splat op

      # TODO Handle nested paths

      # Filter out paths that don't map to this Data Source
      continue unless field = @fields[ns][path]
      @[method] cmdSet, ns, field, conds, args...
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
  set: (cmdSet, ns, field, conds, path, val, ver) ->
    cmd = cmdSet.findOrCreateCommand ns, conds, 'set'
    if field._name == 'Ref'
      pkeyName = field.pkeyName
      unless val[pkeyName] # If is new
        # val is an object literal that needs to be stored as a Mongo doc
        # and then whose pkey needs to be assigned to the current path
        # on the doc defined by the conds lookup in ns
        {Skema: ForeignSkema, source} = field
        targetDoc = new ForeignSkema val
        targetCommandSet = source._oplogToCommandSet targetDoc.oplog
        targetCommand = targetCommandSet.singleCommand
        targetCommand.add targetCommand
        cmdSet.pipe targetCommand.extraAttr(pkeyName), cmd.setAs(path)
        return cmdSet

    val = field.cast val if field.cast

    # Add or augment cmd with cmd.(method|conds|val)
    {ns: cns, method: cmethod, conds: cconds} = cmd

    switch cmethod
      when undefined
        if cconds
          cmd.method = 'update'
          (delta = {})[path] = val
          cmd.val = { $set: delta }
          return cmdSet
        cmd.method = 'insert'
        cmd.val = {}
        cmd.val[path] = val
      when 'update'
        cmd.val.$set[path] = val
      when 'insert'
        cmd.val[path] = val
      else
        throw new Error 'Implement for other incomnig cmethods - e.g., push, pushAll, etc'
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

  push: (ns, field, cmd, conds, path, values...) ->
    values = field.cast values if field.cast
    # Assign or augment cmd.(method|conds|val)
    {ns: cns, method: cmethod, conds: cconds} = cmd
    if cmethod is undefined && cconds is undefined
      cmd.ns = ns
      cmd.method = 'update'
      cmd.conds = conds
      if values.length == 1
        val = values[0]
        k = '$push'
      else if values.length > 1
        val = values
        k = '$pushAll'
      else
        throw new Error "length of 0! Uh oh!"

      (args = {})[path] = val
      (cmd.val ||= {})[k] = args
    else if cmethod == 'update'
      if objEquiv cconds, conds
        if cmd.val.$push
          if existingPush = cmd.val.$push[path]
            cmd.val.$pushAll = {}
            cmd.val.$pushAll[path] = [existingPush, values...]
            delete cmd.val.$push
          else
            nextCommand = {}
            [nextCommand] = @push ns, field, nextCommand, conds, path, values...
        else if cmd.val.$pushAll
          nextCommand = {}
          [nextCommand] = @push ns, field, nextCommand, conds, path, values...
        else
          # Then the prior ops involved something like $set
          nextCommand = {}
          [nextCommand] = @push ns, field, nextCommand, conds, path, values...
      else
        # Current building cmd involves conditions not equal to
        # current op conditions, so create a new cmd
        nextCommand = {}
        [nextCommand] = @push ns, field, nextCommand, conds, path, values...
    else if cmethod == 'insert'
      if ns != cns
        nextCommand = {}
        [nextCommand] = @push ns, field, nextCommand, conds, path, values...
      else
        arr = cmd.val[path] ||= []
        arr.push values...
    else
      nextCommand = {}
      [nextCommand] = @push ns, field, nextCommand, conds, path, val, ver
    return [cmd, nextCommand]

  pop: (ns, field, cmd, path, values..., ver) ->
    throw new Error 'Unimplemented'

operation =
  splat: ([namespace, conds, method, args...]) -> {ns: namespace, conds, method, path: args[0], args}
