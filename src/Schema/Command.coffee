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
        dataSchema = source.dataSchemas[@ns]
        for attrName, attrVal of extraAttrs
          dataField = dataSchema[attrName]
          if dataField.uncast
            attrVal = dataField.uncast attrVal
          doc._doc[attrName] = attrVal

      callback err, extraAttrs
