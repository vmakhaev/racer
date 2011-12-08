module.exports =
  # Takes flattenedPath and traverses the object, assignTo, to the corresponding
  # node. Then, assigns val to this node.
  # @param {Object} assignTo
  # @param {String} flattenedPath
  # @param {Object} val
  assignToUnflattened: (assignTo, flattenedPath, val) ->
    curr      = assignTo
    parts     = flattenedPath.split '.'
    lastIndex = parts.length - 1
    for part, i in parts
      if i == lastIndex
        curr[part] = val
      else
        curr = curr[part] ||= {}
    return curr
