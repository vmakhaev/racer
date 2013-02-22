describe 'OpLog(pathToDoc, opLogManager)', ->
  beforeEach ->
    @opLog = new OpLog

  describe '#listen(fromVer, onOp, cb)', ->
    it 'should return true if listening exists'
    it 'should return false if listening does not exist'
    it 'should cb(null, true) if it succeeds in listening'

  describe '#get(fromVer, toVer, cb)', ->
    it 'should cb(null, ops) where ops are the ops between fromVer and toVer, non-inclusive'
    it 'should cb(null, ops) where ops are all ops after fromVer, if toVer is null'

  describe '#cleanup()', ->
