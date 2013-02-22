describe 'racer behavior', ->
  it 'should have one oplog per document per subscription'

  describe 'subscribed to the same doc via 2 subscriptions', ->
    it 'should not emit the same remote transaction on the model twice'
