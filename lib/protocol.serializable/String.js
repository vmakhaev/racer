var protocol = require('./index');

module.exports = function (String) {
  protocol.serialize.define(String, function (str) {
    return str._at || str;
  });
};
