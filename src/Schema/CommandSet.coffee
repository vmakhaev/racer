Promise = require '../Promise'
{deepEqual} = require '../util'

# @param {Object} opToCommand maps op names -> command generator
CommandSet = module.exports = ->
  @root = null

  # maps hash -> ns -> [Command instances]
  @commands = {}

  # maps command id -> Command instance
  @commandsById = {}

  @commandsByCid = {}

  # We throw op data into here that depends on a cid that we have yet to see.
  @pendingByCid = {}
  return

# CommandSet holds a set of related commands and maintains a 
# dependency graph of commands which is used to fire commands 
# in both a parallel and serial manner upon CommandSet::fire
CommandSet:: =
  positionBefore: (cmdToPos, cmdRel, callback) ->
    @clearPos cmdToPos
    targetPos = cmdRel.pos
    # TODO Fix and test the following if/else logic
    if currPrev = targetPos.prev
      currPrev.next = { prev: currPrev, next: targetPos, cmds: [[cmdToPos, callback]] }
    else
      @root = cmdToPos.pos = targetPos.prev = { next: targetPos, cmds: [[cmdToPos, callback]] }

  pipe: (cmdA, cmdB, callback) ->
    @positionBefore cmdA, cmdB, callback

    # Add cmd to command set if not already part of it
    @index cmdA

  placeAfterPosition: (pos, cmd, singleCallback, prevPosCallback) ->
    @clearPos cmd
    cmdMeta = [cmd, singleCallback, prevPosCallback]
    if currNext = pos.next
      currNext.cmds.push cmdMeta
      cmd.pos = currNext
    else
      cmd.pos = pos.next = { prev: pos, cmds: [cmdMeta] }

  clearPos: (cmdToRemove) ->
    return unless targetPos = cmdToRemove.pos
    {cmds} = targetPos
    for [cmd], i in cmds
      if cmd.id == cmdToRemove.id
        # TODO What if we splice out a callback?
        targetPos.cmds.splice i, 1
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
    @commandsById[id] = command
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
      if currPos.next
        nextProm = new Promise
        currProm.callback ->
          cmd.fire (err, cid, extraAttrs) ->
            return callback err if err
            cb cid, extraAttrs if cb
            nextProm.resolve err, cid, extraAttrs
      else
        currProm.callback ->
          cmd.fire (err, cid, extraAttrs) ->
            return callback err if err
            cb cid, extraAttrs if cb
            callback null
    else
      currPosPromises = (new Promise for _ in cmds)
      nextProm = Promise.parallel currPosPromises...
      currProm.callback ->
        for [cmd, cb, prevPosCb], i in cmds
          currProm.callback prevPosCb if prevPosCb
          cmdPromise = currPosPromises[i]
          cmdPromise.callback cb if cb
          do (cmdPromise) ->
            cmd.fire (err, cid, extraAttrs) ->
              return callback err if err
              cmdPromise.fulfill cid, extraAttrs

    if currPos.next
      @_setupPromises callback, currPos.next, nextProm
    
    return currProm

  fire: (callback) ->
    rootProm = @_setupPromises callback
    rootProm.fulfill()
