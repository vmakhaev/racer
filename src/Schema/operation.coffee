module.exports =
  splat: ([doc, ns, conds, method, args...]) ->
    {doc, ns, conds, method, path: args[0], args}
