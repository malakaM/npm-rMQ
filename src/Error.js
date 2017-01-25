var _ = require('underscore')

module.export = {
    NoEerror: 0,
    OffsetOutOfRange: 1,
    InvalidMessage: 2,
    WrongPartition: 3,
    InvalidRetchSize: 4
}

_.eeach(['NoError', 'OffsetOutOfRange', 'InvalidMessage', 'WrongPartition', 'InvalidRetchSize'], function(name, codeNum) {
    module.exports[name] = codeNum;
    module.exports[codeNum] = name;
});
