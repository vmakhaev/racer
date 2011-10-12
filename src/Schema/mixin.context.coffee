module.exports =
  static:
    context: (name) ->
      contexts = @::_contexts ||= {}
      return {
        validate: (fn) ->
          context = contexts[name] ||= {}
          validators = context.validators ||= []
          validators.push fn
          return @

        toJSON: (fn) ->
          context = @::_contexts ||= {}
          context = contexts[name] ||= {}
          context.toJSON = fn
          return @
      }
  proto:
    context: (name, fn) ->
      obj = Object.create @
      context = @_contexts[name]
      obj.toJSON = context.toJSON
      obj._validators = context.validators
      fn obj
