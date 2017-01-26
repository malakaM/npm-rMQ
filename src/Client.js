var _ = require('underscore'),
    Connection = require('./Connection');

var Client = module.exports = function Client(options) {

    this.connection = new Connection(options);

    this.connect = _.bind(this.connection.connect, this.connection);
    this.disconnect = _.bind(this.connection.disconnect, this.connection);
    this.connecting = _.bind(this.connection.connecting, this.connection);
    this.connected = _.bind(this.connection.connected, this.connection);
    this.close = _.bind(this.connection.close, this.connection);

    // forward all events up through...
    var self = this;
    _.each(this.connection.events, function(name) {
        self.connection.on(name, _.bind(function() {
            var args = Array.prototype.splice.call(arguments, 0);
            args.unshift(name);
            this.emit.apply(this, args);
        }, self));
    });



    return this;
}
