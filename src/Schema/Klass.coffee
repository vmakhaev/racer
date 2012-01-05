Klass = module.exports = -> return

initializing = false
fnTest = if /xyz/.test(-> xyz) then /\b_super\b/ else /.*/

# Example:
# Klass.extend 'CustomKlass',
#   instanceMethodA: ->
#   instancePropertyA: ''
# , staticMethodA: ->
#   staticPropertyA: ''
Klass.extend = (name, instanceConf, staticConf) ->
  ParentKlass = @
  SubKlass = ->
    if !initializing && @init
      @init.apply @, arguments
    return
  _super = @prototype
  initializing = true
  SubKlass:: = proto = new ParentKlass
  initializing = false

  proto.constructor = SubKlass
  SubKlass._name = name # Function::name is immutably ""
  SubKlass.subclasses = []
  SubKlass.superclass = @
  @subclasses.push SubKlass

  for k, v of instanceConf
    proto[k] = if typeof v is 'function' && typeof _super[k] is 'function' && fnTest.test v
                 do (k, v) ->
                   return ->
                     tmp = @_super
                     @_super = _super[k]
                     ret = v.apply @, arguments
                     @_super = tmp
                     return ret
               else
                 v

  for static in ['extend', 'static']
    SubKlass[static] = @[static]

  SubKlass._statics = {}
  for k, v of @_statics
    SubKlass.static k, v
  for k, v of staticConf
    SubKlass.static k, v

  return SubKlass

Klass.subclasses = []

Klass.static = (name, val) ->
  if name.constructor == Object
    for _name, _val of name
      @static _name, _val
    return @

  @_statics[name] = @[name] = val
  # Add to all subclasses
  decorateDescendants = (descendants, name, val) ->
    for SubKlass in descendants
      continue if SubKlass._statics[name]
      SubKlass[name] = val
      decorateDescendants SubKlass.subclasses, name, val
  decorateDescendants @subclasses, name, val
  return @
Klass._statics = {}

Klass.static 'mixin', (mixin) ->
  {init, static, proto} = mixin
  @static static if static
  if proto for k, v of proto
    @::[k] = v
  @_inits.push init if init
