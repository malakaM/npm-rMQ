var _ = require('underscore'),
    Connection = require('./Connection'),
    //make simple
    EventEmitter = require('events').EventEmitter,
    bigint = require('bigint'),
    requestTypes = require('./RequestTypes');

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

    //all events this client deals with (and any subchild)
    this.events = _.union(this.connection.events, [
        'offset',
        'parseerror',
        'messageerror',
        'lastoffset',
        'lastmessage',
        'message'
    ]);

    this.options = _.defaults(options, {
        maxSize: 1048576, //1MB
        autoConnectOnWrite: false,
        sendQueueLength: 10000
    });

    this.fetch_defauts = {
        type: requestTypes.PRODUCE,
        encode: function(t) {
            return this._encodeSendRequest(t)
        },
        partition: 0
    }

    this._buffer = new Buffer(this.options.maxSize);
    this.fetch_defaults.encode = this.fetch_defaults.encode.bind(this);
    this.offset_defaults.encode = this.offset_defaults.encode.bind(this);
    this.send_defaults.encode = this.send_defaults.encode.bind(this);

    this._autoConnectOnWrite = this.options.autoCnnectOnWrite;
    this._sendQueueLength = this.options.sendQueueLength;

    this.on('connected', _.bind(this._connected, this));

    this._msgs_requested = 0;
    this._msgs_sent = 0;
    this._msgs_dropped = 0;


    this._reset();
    this.cleanRequests();

    return this;
};

/**
 * Inherit from 'EvenrEmitter.prototype'.
 */
Client.prototype._proto_ = EventEmitter.prototype;

/**
 * Get the statics for the current Client
 * include message count, buffer size, etc.
 *
 * @return (mixed) The stats for the current client
 *
 * @example
 *    client.getStats(function(d){
 *      console.log(d.host);
 *      console.log(d.port);
 *      console.log(d.bufferSize);
 *    });
 */
Client.prototype.getStats = function() {
    return {
        host: this.connection.options.host,
        port: this.connection.options.port,
        bufferSize: this.connection.connected() ? this.connection.bufferSize : "n/a"

    };
}

Client.prototype.getMaxFetchSize = function() {
    return this.options.maxSize;
}

Client.prototype.clearRequests = function() {
    this._requests = [];
    this._sendRequests = [];
}

/**
 * Fetchs the specified topics
 * @param {[type]} args [description]
 * @return {[type]}  [description]
 */
Client.prototype.fetchTopic = function(args) {
    var request = null;
    if (_.isObject(args)) {
        request = _.defaults(args, this.fetch_defauts)
    } else if (_.isString(args)) {
        request = _.defaults({
            name: args
        }, this.fetch_Defaults);
    } else {
        throw new Error('Fetch topic invalid parameter, must be name or complex object');
    }

    request.original_offset = bigint(request.offset);
    request.bytesRead = 0;
    this._pushRequest({
        request: request
    });
    return this;
}

Client.prototype.fetchOffsets = function(args) {
    var request = null;
    if (_.isObject(args)) {
        request = _.defaults(args, this.offset_defauts);
    } else if (_.isString(args)) {
        request = _.defaults({
            name: args
        }, this.offset_defauts);
    } else {
        throw new Error('Fetch topic invalid parameter, must be name or complex object');
    }

    this._pushRequest({
        requst: request
    });
    return this;
}

Client.prototype.send = function(args, message, callback) {
    var request = null;
    if (_.isObject(args)) {
        request = _.defaults(args, this.send_defaults);
    } else if (_.isString(args)) {
        request = _.defaults(topic: args, messages: messages, this.send_defaults);
    } else {
        throw new Error('Fetch topic invalid parameter, must be name or complex object');
    }

    if (false == _.isArray(request.messages)) {
        request.messages = [request.messages];
    }

    var cb = _.bind(function() {
        this._msgs_sent++;
        callback && callback();
    }, this);

    this._msgs_requested++;
    this._pushRequest({
        request: request,
        callback: cb
    });
    return this;
}

Client.prototype._connected = function() {
    this._reset();

    //handle data from the connection
    this.connection.transport.on('data', _.bind(this._onData, this));

    //send queued send requests
    //make a copy because socket write may return immediately and modify the size
    var r = this._sendRequests.slice(0);
    for (var i = 0; i < r.length; i++) {
        this._writeRequest(r[i]);
    }
    // send queued read requestTypes
    for (var i = 0; i < this._requests.length; i++) {
        this._writeRequest(this._requests[i]);
    }
}

Client.prototype._pushSendRequest = function(requestObj) {
    var cb = requestObj.callback;
    requestObj.callback = _.bind(function() {
        this._sendRequests.shift();
        cb && cb();
    }, this);

    // drop entries if too long
    if (this._sedRequests.length >= this._sendQueueLength) {
        this._msgs_dropped++;
        this._sendRequests.shift();
    }
    this._sendRequests.push(requestObj);
    this._writeRequest(requestObj);
}
