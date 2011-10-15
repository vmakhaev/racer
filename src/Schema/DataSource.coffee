DataSource = module.exports = ->
  @fields = {}
  return

DataSource::=
  addField: (field, config) ->
    @fields[field] = if config == true
      'direct'
    else
      config

  applyOps: (oplog, callback) ->
    oplog = @_minifyOps oplog if @_minifyOps
    queries = @_queriesForOps oplog
    remainingQueries = queries.length
    for {method, args} in queries
      # e.g., adapter.update {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
      adapter[method] args..., (err) ->
        --remainingQueries || callback() if callback
