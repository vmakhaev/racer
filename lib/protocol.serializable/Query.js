var protocol = require('./index');

module.exports = function (Query) {
  protocol.serialize.define(Query, function (query) {
  });
};
