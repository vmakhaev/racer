module.exports =
  mergeAll: (to, froms...) ->
    for from in froms
      to[key] = value for key, value of from
    return to

  merge: (to, from) ->
    to[key] = value for key, value of from
    return to

  hasKeys: (obj, ignore) ->
    for key of obj
      continue if key is ignore
      return true
    return false

  curry: (fn, args...) ->
    return (remArgs...)-> fn.apply null, args.concat remArgs

  # Ported to coffeescript from node.js assert.js
  deepEqual: deepEqual = (actual, expected) ->
      # 7.1. All identical values are equivalent, as determined by ==.
    return true if actual == expected

    if Buffer.isBuffer(actual) && Buffer.isBuffer(expected)
      return false if actual.length != expected.length

      for actualVal, i in actual
        return false if actualVal != expected[i]

      return true

    # 7.2. If the expected value is a Date object, the actual value is
    # equivalent if it is also a Date object that refers to the same time.
    if actual instanceof Date && expected instanceof Date
      return actual.getTime() == expected.getTime()

    # 7.3. Other pairs that do not both pass typeof value == 'object',
    # equivalence is determined by ==.
    if typeof actual != 'object' && typeof expected != 'object'
      return actual == expected

    # 7.4. For all other Object pairs, including Array objects, equivalence is
    # determined by having the same number of owned properties (as verified
    # with Object.prototype.hasOwnProperty.call), the same set of keys
    # (although not necessarily the same order), equivalent values for every
    # corresponding key, and an identical 'prototype' property. Note: this
    # accounts for both named and indexed properties on Arrays.
    return objEquiv actual, expected

  # Ported to coffeescript from node.js assert.js
  objEquiv: objEquiv = (a, b) ->
    if !a? && !b?
      # if isUndefinedOrNull(a) || isUndefinedOrNull(b)
      return false
    # an identical 'prototype' property.
    return false if a:: != b::
    #~~~I've managed to break Object.keys through screwy arguments passing.
    #   Converting to array solves the problem.
    if isArguments a
      return false unless isArguments b
      a = pSlice.call a
      b = pSlice.call b
      return deepEqual a, b
    try
      ka = Object.keys a
      kb = Object.keys b
    catch e #happens when one is a string literal and the other isn't
      return false
    # having the same number of owned properties (keys incorporates
    # hasOwnProperty)
    return false if ka.length != kb.length
    #the same set of keys (although not necessarily the same order),
    ka.sort()
    kb.sort()
    #~~~cheap key test
    i = ka.length
    while i--
      return false if (ka[i] != kb[i])
    #equivalent values for every corresponding key, and
    #~~~possibly expensive deep test
    i = ka.length
    while i--
      key = ka[i]
      return false unless deepEqual a[key], b[key]
    return true

  # TODO Test this
  deepCopy: (obj) ->
    if obj.constructor == Object
      ret = {}
      ret[k] = deepCopy v for k, v of obj
      return ret
    if Array.isArray obj
      ret = []
      ret.push deepCopy v for v in obj
      return ret
    return obj

isArguments = (object) ->
  Object::toString.call(object) == '[object Arguments]'
