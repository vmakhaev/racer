Command = module.exports = (@ns, @conds, @opMethod) ->
  return

Command.pipe = (commandOnePromise, commandTwoAction) ->
  @commandSet

Command.parallel = ->

Command:: =
  extraAttr: (attrName) ->
    return { qid: @id, method: extraAttr, attr: attrName }

  setAs: (attrName) ->

  appendTo: (attrName) ->

  parallel: (simultCommand) ->

  placeBefore: (nextCommand) ->

  placeAfter: (priorCommand) ->

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

  fire: (dataSource, callback) ->
    args = @args
    adapter = dataSource.adapter
    # e.g., adapter.update 'users', {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
    adapter[@method] args..., (err, extraAttrs) =>
      return callback err if err

      return unless extraAttrs
      # Phase 1: Cast extraAttrs from db data to logical
      # schema attributes using information about the fields
      # from the Data Source and the Schema
      for attrName, attrVal of extraAttrs
        sourceField = @sourceFields[attrName]
        logicalField = @logicalFields[attrName]
        logicalType = logicalField.logicalType
        logicalTypeName = logicalType.name || logicalType._name
        if sourceField._name != logicalTypeName
          extraAttrs[attrName] = sourceField['to' + logicalTypeName](attrVal)

      callback null, extraAttrs
