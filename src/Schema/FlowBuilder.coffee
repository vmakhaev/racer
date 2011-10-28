FlowBuilder = exports.FlowBuilder = (@LogicalSkema) ->
  @flow = []
  return

FlowBuilder:: =
  first: (sources..., callback) ->
    if 'function' != typeof callback
      sources.push callback
      callback = null
    @flow.push [sources, callback]
    return @

FlowBuilder::then = FlowBuilder::first

# TODO FlowBuilder::before/after/btwn?
