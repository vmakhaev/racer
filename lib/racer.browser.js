/** WARNING
 * All racer modules for the browser should be included in racer.coffee and not
 * in this file.
 */

// Static isReady and model variables are used, so that the ready function can
// be called anonymously. This assumes that only one instance of Racer is
// running, which should be the case in the browser.
var IS_READY
  , model;

var reconnect = require('reconnect');

exports = module.exports = plugin;
exports.useWith = { server: false, browser: true };
exports.decorate = 'racer';

function plugin (racer) {
  racer.init = function (tuple, socket) {
    var clientId  = tuple[0]

        // TODO Repalce memory.version with memory.versions
      , memory    = tuple[1]
      , count     = tuple[2]
      , onLoad    = tuple[3]
      , startId   = tuple[4]
      , ioUri     = tuple[5]
      , ioOptions = tuple[6];

    model = new this.protected.Model;
    model._clientId = clientId;
    model._startId  = startId;
    model._memory.init(memory);
    model._count = count;

    for (var i = 0, l = onLoad.length; i < l; i++) {
      var item = onLoad[i]
        , method = item.shift();
      model[method].apply(model, item);
    }

    racer.emit('init', model);

    reconnect(function (stream) {
      // We get a new stream for every reconnect event
      stream.pipe(model.createStream()).pipe(stream);
    }).connect('/ws?clientId=' + clientId);

    IS_READY = true;
    racer.emit('ready', model);

    return racer;
  };

  racer.ready = function (onready) {
    return function () {
      if (IS_READY) return onready(model);
      racer.on('ready', onready);
    };
  }
}
