var _ = require('underscore'),
    net = require('net')
EventEmitter = require('events').EventEmitter;

var Connection = module.exports = function Connection(options) {
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
