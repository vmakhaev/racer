MongoDataField = module.exports = DataField.extend 'MongoDataField',
  set: (val) ->
    @owner.set(@name, val)
    return @owner.save()

  set: (val) ->
    mongo = @owner.dataSource
    mongo.update # or mongo.insert
