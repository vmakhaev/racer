Promise = require '../Promise'

# TODO Should we replace @source, @ns with @dataSkema ?
# @param {DataSource} source
# @param {String} ns is the namespace relative to the data source
# @param {Object} conds
# @param {Schema} doc
Command = module.exports = (@source, @ns, @conds, @doc) ->
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
  fire: (callback) ->
    source = @source
    @compile()
    args = @args
    # e.g., adapter.update 'users', {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
    return source.adapter[@method] args..., (err, extraAttrs) =>
      if doc = @doc
        # Transform data schema attributes from db result 
        # into logical schema attributes
        dataSchema = source.dataSchemasWithNs[@ns]
        for attrName, attrVal of extraAttrs
          dataField = dataSchema.fields[attrName]
          if dataField.type?.uncast
            attrVal = dataField.type.uncast attrVal
          doc._doc[attrName] = attrVal

      callback err, extraAttrs
