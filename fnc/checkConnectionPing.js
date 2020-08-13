const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');
var mqtt = require('mqtt')

let SaveDataToSQLServer = require('./saveDataToSQLServer.js')

let arrAllData = []
module.exports = async function(){
  arrAllSites.forEach(function(site){
    ping.sys.probe(site.ip, function(isAlive){
      if (isAlive) {
        saveConnectionStatus(site.site_id, site.site_id, 1)
        log.info('PING - '+ site.site_id + ' - IP: ' + site.ip + ' successfully')
      }else{
        saveConnectionStatus(site.site_id, site.site_id, 0)
        log.info('PING - '+ site.site_id + ' - IP: ' + site.ip + ' failed')
      }
    });
  });
}


