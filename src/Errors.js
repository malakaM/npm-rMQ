var _ = require('underscore')

module.exports = {
    NoError: 0,
    OffsetOutOfRange: 1,
    InvalidMessage: 2,
    WrongPartition: 3,
    InvalidRetchSize: 4
}

_.each(['NoError', 'OffsetOutOfRange', 'InvalidMessage', 'WrongPartition', 'InvalidRetchSize'], function(name, codeNum) {
    module.exports[name] = codeNum
    module.exports[codeNum] = name
})
