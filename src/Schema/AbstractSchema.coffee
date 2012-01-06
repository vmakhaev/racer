module.exports = AbstractSchema = ->

AbstractSchema.castObj = (obj) ->
  fields = @fields
  for path, val of obj
    field = fields[path]
    obj[path] = field.cast val if field.cast
  return obj
