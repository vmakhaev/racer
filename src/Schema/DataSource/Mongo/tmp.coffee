oplog = [op]
op = {ns/schema, conds, method, path|args}

# 8
cmd = parallel cmdA, cmdB

cmd.fire (err, cmdResA, cmdResB) ->
  # DO SOMETHING

# 9

cmdA1 = unshift v
cmdA2 = pop
cmdB = insert

seqA2toB = bind cmdA2.from('result'), cmdB.to('args')

bind = (attrA, attrB) ->

cmdA2.pipe (result) ->
  cmdB.args = result
  cmdB.fire()

cmdA = parallel cmdA1, cmdA2toB

cmdA.fire (err, [cmdA1out, cmdBout]) ->


# For Ref
  cmdB.callback (err, extraAttrs) ->
    cmdA.val._id = extraAttrs
    cmdA.fire (err, extraAttrs) ->
      callback

  cmdB.fire()
