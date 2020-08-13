const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');
var mqtt = require('mqtt')

let SaveDataToSQLServer = require('./saveDataToSQLServer.js')

module.exports = async function(tag_header, site_id, is_connect){
  let TempData = [{
              tag_header: tag_header,
              site_id : site_id,
              timestamp: new Date(), //(data_row.TimeStr),
              tagname: 'COMMUNICATION',
              value: parseFloat(is_connect),
              created_at: new Date(),
            }]
  SaveDataToSQLServer(TempData)
}


