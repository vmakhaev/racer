{merge} = require '../../util'
DataQueryBuilder = require './QueryBuilder'

FindBuilder = module.exports = (@source, @conds) ->
  DataQueryBuilder.call @, 'find'
  return

FindBuilder:: = merge new DataQueryBuilder('find'),
  queryCallback: (err, arr) ->
    fields = @_includeFields
    for path, field of fields
      if field.type.isPkey
        pkeyPath = path
        break
    throw new Error 'Missing pkey path' unless pkeyPath

    # Adds search results, e.g.,
    #   [ {_id: 10, a: 1, b: 2}, {_id: 20, a: 3, b: 4}, ...]
    # to the index `resolveToByPath`
    #   { a:   [{val: 1,  pkeyVal: 10}, {val: 3,  pkeyVal: 20}, ...], 
    #     b:   [{val: 2,  pkeyVal: 10}, {val: 4,  pkeyVal: 20}, ...],
    #     _id: [{val: 10, pkeyVal: 10}, {val: 20, pkeyVal: 20}, ...] }
    resolveToByPath = {}
    for member, i in arr
      pkeyVal = member[pkeyPath]
      for path, val of member
        resolveToByPath[path] ||= []
        resolveToByPath[path][i] = {val, pkeyVal}
    fieldPromises = @_fieldPromises
    for path, promise of fieldPromises
      promise.resolve err, resolveToByPath[path], fields[path]
