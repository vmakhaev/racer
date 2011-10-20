Promise = require '../Promise'
Command = require './Command'
{deepEqual} = require '../util'

# @param {Object} opToCommand maps op names -> command generator
CommandSet = module.exports = ->
  @root = null

  # maps ns -> opMethod -> Command instance
  @commands = {}

  # maps command id -> Command instance
  @commandsById = {}

  @commandsByCid = {}
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

  findCommandByCid: (cid) ->
    @commandsByCid[cid]

  findOrCreateCommand: (ns, conds, opMethod, doc) ->
    commands = @commands
    nsQueries = commands[ns] ||= {}
    if opmQueries = nsQueries[opMethod]
      for command in opmQueries
        {conds: qconds} = command
        return command if deepEqual conds, qconds

    command = new Command ns, conds, opMethod, doc
    @index command

    # Position within concurrent flow control data structure
    unless @root
      command.pos = @root = { cmds: [[command, null]] }
    else
      @root.cmds.push [command, null]
      command.pos = @root

    return command

  index: (command) ->
    {ns, opMethod} = command
    if @singleCommand is undefined
      @singleCommand = command
    else if @singleCommand isnt false
      @singleCommand = false
    nsQueries = @commands[ns] ||= {}
    opmQueries = nsQueries[opMethod] ||= []
    index = opmQueries.push command
    id = command.id = ns + opMethod + index
    @commandsById[id] = command
    @commandsByCid[cid] = command if cid = command.cid
    return true

  _setupPromises: (source, callback, currPos = @root, currProm = new Promise) ->
    cmds = currPos.cmds

    if cmds.length == 1
      [cmd, cb] = cmds[0]
      if currPos.next
        nextProm = new Promise
        currProm.callback ->
          cmd.fire source, (err, extraAttrs) ->
            return callback err if err
            cb extraAttrs if cb
            nextProm.resolve err, extraAttrs
      else
        currProm.callback ->
          cmd.fire source, (err, extraAttrs) ->
            return callback err if err
            cb extraAttrs if cb
            callback null
    else
      throw new Error 'Unimplemented'

    if currPos.next
      @_setupPromises source, callback, currPos.next, nextProm
    
    return currProm

  fire: (source, callback) ->
    rootProm = @_setupPromises source, callback
    rootProm.fulfill()
