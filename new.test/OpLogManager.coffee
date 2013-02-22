OpLogManager = require '../lib/OpLogManager'
OpLog = require '../lib/OpLog'

describe 'OpLogManager(opLogStore)', ->
  beforeEach ->
    @opLogMgr = new OpLogManager

  describe '#get(docPath)', ->
    it 'should return null if OpLog for doc does not exist', ->
      expect(@opLogMgr.get(pathToDoc)).to.equal null

    it 'should return a unique OpLog for a doc', ->
      opLog = @opLogMgr.add(pathToDoc)
      expect(@opLogMgr.get(pathToDoc)).to.equal opLog

  describe '#add(docPath)', ->
    it 'should return true if added', ->
      expect(@opLogMgr.add(pathToDoc)).to.equal true

    it 'should return false if OpLog is already added', ->
      @opLogMgr.add(pathToDoc)
      expect(@opLogMgr.add(pathToDoc)).to.equal false

  describe '#rm(docPath)', ->
    it 'should return the removed OpLog', ->
      @opLogMgr.add(pathToDoc)
      expect(@opLogMgr.rm(pathToDoc)).to.be.an.instanceOf OpLog

    it 'should result in subsequent OpLog#get(docPath) returning null', ->
      @opLogMgr.add(pathToDoc)
      @OpLogManager.rm(pathToDoc)
      expect(@opLogMgr.get(pathToDoc)).to.equal null
