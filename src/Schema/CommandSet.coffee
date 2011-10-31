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

# TODO Eventually, this should sit above the
#      Data Source layer, to be able to work
#      across different data sources
# CommandSet holds a set of related commands and maintains a 
# dependency graph of commands which is used to fire commands 
# in both a parallel and serial manner upon CommandSet::fire
CommandSet:: =
  positionBefore: (cmdToPos, cmdRel, callback) ->
    if subjectPos = cmdToPos.pos
      {cmds} = subjectPos
      for [cmd], i in cmds
        if cmd.id == cmdToPos.id
          # TODO What if we splice out a callback?
          subjectPos.cmds.splice i, 1
          break

    targetPos = cmdRel.pos
    if currPrev = targetPos.prev
      currPrev.next = { prev: currPrev, next: targetPos, cmds: [[cmdToPos, callback]] }
    else
      @root = cmdToPos.pos = targetPos.prev = { next: targetPos, cmds: [[cmdToPos, callback]] }

  pipe: (cmdA, cmdB, callback) ->
    @positionBefore cmdA, cmdB, callback

    # Add cmd to command set if not already part of it
    @index cmdA

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
    if cmds.length == 1
      [cmd, cb] = cmds[0]
      if currPos.next
        nextProm = new Promise
        currProm.callback ->
          cmd.fire (err, extraAttrs) ->
            return callback err if err
            cb extraAttrs if cb
            nextProm.resolve err, extraAttrs
      else
        currProm.callback ->
          cmd.fire (err, extraAttrs) ->
            return callback err if err
            cb extraAttrs if cb
            callback null
    else
      console.log require('util').inspect cmds, false, 3
      throw new Error 'Unimplemented'

    if currPos.next
      @_setupPromises callback, currPos.next, nextProm
    
    return currProm

  fire: (callback) ->
    rootProm = @_setupPromises callback
    rootProm.fulfill()
