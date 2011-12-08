operation = require './operation'

CommandSequence = module.exports = ->
  return

# TODO How to deal with data dependencies?
CommandSequence:: =
  add: (cmd, conds, phase) ->
    node = @findPosition conds, phase

  matchingCmd: (dField, conds, phase) ->
    lField = dField.logicalField
    lField.name

CommandSequence.fromOplog = (oplog, schemasByNs) ->
  cmdSeq = new @
  for arr in oplog
    {doc, ns, path, conds, method, args} = op = operation.fromArray arr
    logicalField = schemasByNs[ns].fields[path]

    method = op.method
    intent = switch method
      when 'find', 'findOne' then 'query'
      else                        method
    dataFlow = logicalField.flow[intent]
    dataFlow.traverse (phase, dField) ->
      # TODO Translate conds for data schema via casting and field name transformation
      if cmd = cmdSeq.matchingCmd dField, conds, phase
        cmd.add dField, args
      else
        cmd = Command.create conds, dField, args
        cmdSeq.add cmd, conds, phase

  return cmdSeq
