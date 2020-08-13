require('dotenv').config()
const mkdirp = require('mkdirp');

const directoryPath = process.env.CSV_FLEXY_PATH;
const logFolder = directoryPath + '\\Log\\';
mkdirp.sync(logFolder);
//********************************************************************************
// Global variable
const opts = {
    // errorEventName:'error',
        logDirectory:logFolder, // NOTE: folder must exist and be writable...
        fileNamePattern:'log-<DATE>.log',
        dateFormat:'YYYY.MM.DD HH'
};

module.exports = require('simple-node-logger').createRollingFileLogger( opts );
