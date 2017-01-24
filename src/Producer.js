var _ = require('underscore'),
    Connection = require('./Connection');

var Producer = module.exports = function Producer(options) {

    this.connection = new Connection(options);

}
