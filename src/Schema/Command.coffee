Promise = require '../Promise'

Command = module.exports = (@ns, @conds, @doc) ->
  @cid = cid if cid = @conds?.__cid__
  @method
  @args
  return

Command:: =
  # Better to build a command out of multiple ops using
  # a pre-compiled form; then post-compile the command for
  # use by the adapter once the command is done being built.
  # Pre-compiled will look like:
  #   command.ns
  #   command.method
  #   command.conds
  #   command.val
  #   command.opts
  # Post-compiled will look like:
  #   command.method
  #   command.args = [command.ns, command.conds, command.val, command.opts]
  compile: ->
    args = @args = [@ns]
    opts = @opts ||= {}
    opts.safe = true
    if @method == 'update'
      opts.upsert = true
      args.push @conds
    args.push @val, opts
    return @

  # Dispatches the command
  fire: (source, callback) ->
    @compile()
    args = @args
    # e.g., adapter.update 'users', {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
    return source.adapter[@method] args..., (err, extraAttrs) =>
      if doc = @doc
        # Transform data schema attributes from db result 
        # into logical schema attributes
        LogicalSkema = source.schemas[@ns]
        nsFields = source.fields[@ns]
        for attrName, attrVal of extraAttrs
          dataField = nsFields[attrName]
          logicalField = LogicalSkema.fields[attrName]
          logicalType = logicalField.type
          logicalTypeName = logicalType.name || logicalType._name
          if dataField._name != logicalTypeName
            attrVal = dataField['to' + logicalTypeName](attrVal)
          doc._doc[attrName] = attrVal

      callback err, extraAttrs

#
#
#    adapter[@method] args..., (err, extraAttrs) =>
#      return @promise.resolve err, extraAttrs
#
#
#      return callback err if err
#
#      return unless extraAttrs
#      # Phase 1: Cast extraAttrs from db data to logical
#      # schema attributes using information about the fields
#      # from the Data Source and the Schema
#      for attrName, attrVal of extraAttrs
#        sourceField = @sourceFields[attrName]
#        logicalField = @logicalFields[attrName]
#        logicalType = logicalField.logicalType
#        logicalTypeName = logicalType.name || logicalType._name
#        if sourceField._name != logicalTypeName
#          extraAttrs[attrName] = sourceField['to' + logicalTypeName](attrVal)
#
#      callback null, extraAttrs
