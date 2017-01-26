var _ = require('underscore'),
    net = require('net')
EventEmitter = require('events').EventEmitter;

var Connection = module.exports = function Connection(options) {
    this._ctor(options);
    this.events = ['disconnected',
        'connection_error',
        'closed',
        'connect',
        'connected',
        'connecting',
        'error',
        'end'
    ];

    return this;
}

/**
 * Inherit from 'EventEmitter.prototype'
 */

Connection.prototype._proto_ = EventEmitter.prototype;

Connection.prototype._ctor = function(options) {
    this.connection_defaults = {
        host: "127.0.0.1",
        port: 9876
    }
    this.options = _.defaults(options, this.connection_defaults);
    this.active = false;
}

/**
 * Get the currently configured address for rocketmq connection
 *
 * @return{String} The current rocketmq url
 */
Connection.prototype._address = function() {
    return "rocketmq://" + this.options.host + ":" + this.options.port;
}

/**
 * Makes the dedicated connection to the rocketmq instance.
 *
 * @param {Function} callback Callback to get the rocketmq connection
 * @return {Connection} Returns itself as part of fluent interface style
 */
Connection.prototype.connect = function(callback) {
    if (this.active) {
        throw new Error('cannot call connect more than once.');
    }

    this.active = true;

    this.transport = net.createConnection(this.options.port, this.options.host);
    if (!this.transport) {
        this.emit('connection_error', this._addess(), "Could not create socket");
        this.emit('closed', this_address());
        return this;
    }
    this.transport.on('connect', _.bind(this._handleSocketConnect, this));
    this.transport.on('error', _.bind(this._handleSocketError, this));
    this.transport.on('end', _.bind(this._handleSocketEnd, this));

    if (callback != undefined) {
        this.transport.on('connect', _bind(function() {
            callback(this, _addess())
        }, this));
    }
    this.emit("connecting", this._address());
    return this;
}

/**
 * Closes the current connection.
 * @return {Connection}
 */
Connection.prototype.close = function() {
    if (this.connected()) {
        this.transport.end();
    }
    delete this.transport;
    this.emit('closed', this._address());;
    return this;
}

/**
 * Disconnects from the server.
 * @return {Connection}
 */
Connection.prototype.disconnect = function() {
    this.close(false);
    return this;
}

/**
 * Check if connection attempt is being made.
 * @return {boolean} True or false depending on if connection attempt is being made
 */
Coonnection.prototype.connecting = function() {
    return this.transport != null && this.transport._connecting;
}

/**
 * Checks if connection attempt has been negotiated
 * @return {boolean} True or false depending on if connection as been negotiated
 */
Connection.prototype.connected = function() {
    return this.transport != null && !this.transport._connecting;
}

Connection.prototype._handleSockedEnd = function() {
    this.emit("diconnected", this._address());
}

Connection.prototype._handleSocketError = function() {
    this.emit("connection_error", this._address(), error);
    this.close();
}

Connection.prototype._handleSocketConnect = function() {
    this.transport.on('close', _.bind(this.close, this));
    this.emit("connected", this._address());
}
