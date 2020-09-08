const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');

let beforeXXdays
let beforeday
module.exports = async function(tableName, days){
  sql.connect(sqlConfig, function (err) {
    if (err){
      console.log(err);
    } 
    else
    {
      let request = new sql.Request();
      beforeXXdays = moment().subtract(days, 'days');
      beforeday = new Date(beforeXXdays)
      //console.log('data', beforeday)
      request.input('beforeday', sql.DateTimeOffset, beforeday);

      request.query('DELETE FROM ' + tableName + ' WHERE flag IS NULL AND created_at < @beforeday', function(err, recordsets) {  
        if (err) console.log(err); 
      });
    }
  })
  sql.on('error', err => {
    //console.log('SQL has error when delete data ', err.message )
    log.error('SQL has error when delete data: ' + err.message)
  })
}
