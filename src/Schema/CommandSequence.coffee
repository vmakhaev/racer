# CommandSequence holds a set of related commands and maintains a 
# dependency graph of commands which is used to fire those commands
# in both a parallel and serial manner upon CommandSequence::fire
#
# We use it from Logical/Schema.coffee via:
#     CommandSequence.fromOplog(oplog, schemas).fire (err, cid, extraAttrs) ->

Promise = require '../Promise'
{deepEqual} = require '../util'
operation = require './Logical/operation'

CommandSequence = module.exports = ->
  @root          = null
  @commands      = {} # maps hash -> ns -> [Command instances]
  @commandsById  = {} # maps command id -> Command instance
  @commandsByCid = {}
  @pendingByCid  = {} # Contains op data dependent on cid's we have yet to see
  return

# Keeping this as a separate function makes testing oplog to
# command sequence possible.
# TODO This should be able to handle more refined write flow control
# TODO Handle nested paths
CommandSequence.fromOplog = (oplog, schemasByNs) ->
  cmdSeq = new CommandSequence
  for op in oplog
    {doc, ns, conds, method, path, args} = operation.splat op
    LogicalSkema = schemasByNs[ns]
    {dataFields} = logicalField = LogicalSkema.fields[path]
    # TODO How to modify for STM? Need rollback mechanism
    for dataField in dataFields
      {source} = dataField
      # In order to modify cmdSeq, we delegate to the data
      # source, which delegates the appropriate data type
      source[method] cmdSeq, doc, dataField, conds, args...
  return cmdSeq

CommandSequence:: =
  # TODO Replace with `position cmd, before: fixedCmd, callback`
  # TODO Add tests
  # @param {Array}    movingCmd
  # @param {Array}    fixedCmd
  # @param {Function} callback
  positionBefore: (movingCmd, fixedCmd, callback) ->
    @clearPos movingCmd
    fixedPos = fixedCmd.pos
    if currPrev = fixedPos.prev
      currPrev.next = movingCmd.pos = { prev: currPrev, next: fixedPos, cmds: [[movingCmd, callback]] }
    else
      @root = movingCmd.pos = fixedPos.prev = { next: fixedPos, cmds: [[movingCmd, callback]] }

  pipe: (cmdA, cmdB, callback) ->
    @positionBefore cmdA, cmdB, callback
    @index cmdA # Add cmd to command set if not already part of it

  placeAfterPosition: (pos, cmd, singleCallback, prevPosCallback) ->
    @clearPos cmd
    cmdMeta = [cmd, singleCallback, prevPosCallback]
    if currNext = pos.next
      currNext.cmds.push cmdMeta
      cmd.pos = currNext
    else
      cmd.pos = pos.next = { prev: pos, cmds: [cmdMeta] }

  clearPos: (cmd) ->
    return unless pos = cmd.pos
    delete cmd.pos
    {cmds} = pos
    for [currCmd], i in cmds
      if currCmd.id == cmd.id
        # TODO What if we splice out a callback?
        pos.cmds.splice i, 1
        return

  # As an alternative to isMatchPredicate, we could subclass Command as MongoCommand and place the logic inside
  # boolean method MongoCommand::doesMatch(opMethod, otherParams...)
  findCommand: (ns, conds, isMatchPredicate) ->
    cmds = @commands[ns]
    return unless cmds
    for cmd in cmds
      cmdConds = cmd.conds
      continue if (conds == undefined || cmdConds == undefined) && conds != cmdConds
      continue unless deepEqual conds, cmdConds
      return cmd if isMatchPredicate cmd

  index: (command) ->
    {ns, conds} = command
    if @singleCommand is undefined
      @singleCommand = command
    else if @singleCommand isnt false
      @singleCommand = false
    commands = @commands[ns] ||= []
    index = commands.push command
    id = command.id = command.ns + '.' + index
    @commandsById[id]   = command
    @commandsByCid[cid] = command if cid = command.cid
    return true

  position: (command) ->
    # Position within concurrent flow control data structure
    unless @root
      @root = { cmds: [[command, null]] }
    else
      @root.cmds.push [command, null]
    command.pos = @root

  _setupPromises: (callback, currPos = @root, currProm = new Promise) ->
    cmds = currPos.cmds
    # console.log cmd for cmd in cmds
    if cmds.length == 1
      [cmd, cb, prevPosCb] = cmds[0]
      currProm.callback prevPosCb if prevPosCb
      if nextPos = currPos.next
        nextProm = new Promise
        @_setupPromises callback, nextPos, nextProm
      currProm.callback ->
        cmd.fire (err, cid, extraAttrs) ->
          return callback err if err
          cb cid, extraAttrs if cb
          if nextPos
            nextProm.resolve err, cid, extraAttrs
          else
            callback null
    else
      currPosPromises = (new Promise for _ in cmds)
      posPromise = Promise.parallel currPosPromises
      if nextPos = currPos.next
        nextProm = posPromise
        @_setupPromises callback, nextPos, nextProm
      else
        posPromise.bothback (err) ->
          return callback err if err
          callback null
      currProm.callback ->
        for [cmd, cb, prevPosCb], i in cmds
          currProm.callback prevPosCb if prevPosCb
          cmdPromise = currPosPromises[i]
          cmdPromise.callback cb if cb
          do (cmdPromise) ->
            cmd.fire (err, cid, extraAttrs) ->
              return callback err if err
              cmdPromise.fulfill cid, extraAttrs

    return currProm

  fire: (callback) ->
    rootProm = @_setupPromises callback
    rootProm.fulfill()

