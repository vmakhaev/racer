Promise = require '../../Promise'

# @param {Object} opToCommand maps op names -> command generator
CommandSet = module.exports = ->
  @flowchart = []

  # maps ns -> opMethod -> Command instance
  @queries = {}

  # maps command id -> Command instance
  @queriesById = {}
  return

# TODO Eventually, this should sit above the
#      Data Source layer, to be able to work
#      across different data sources
#
# CommandSet holds a set of related queries and maintains a 
# dependency graph of queries which is used to fire queries 
# in both a parallel and serial manner upon CommandSet::fire
CommandSet:: =
  findOrCreateCommand: (ns, conds, opMethod) ->
    queries = @queries
    nsQueries = queries[ns] ||= {}
    if opmQueries = nsQueries[opMethod]
      for {conds: qconds} in opmQueries
        return command if objEquiv conds, qconds
    else
      opmQueries = nsQueries[opm] = []
    command = new Command ns, conds, opMethod
    @add command
    return command

  add: (command) ->
    {ns, opMethod} = command
    if @singleCommand is undefined
      @singleCommand = command
    else if singleCommand isnt false
      @singleCommand = false
    nsQueries = @queries[ns] ||= {}
    opmQueries = nsQueries[opMethod] ||= []
    index = opmQueries.push command
    id = command.id = ns + opMethod + index
    @queriesById[id] = command
    return true

  pipe: (from, to) ->
    {qid: qidFrom, method: methodFrom, attr: attrFrom} = from
    {qid: qidTo, method: methodTo, attr: attrTo} = to

  fire: (adapter, callback) ->
    # TODO Lazy compile queries here
    prom = do setupPromises (flow = @flowchart) ->
      curr = null
      return new Promise flow if flow.length == 1
      # Step serially through the flow chart data structure
      for unit in flow
        if Array.isArray unit
          if unit.length == 1
            curr = new Promise unit
          else
            curr = #
        else

    rootCommand.fire adapter
