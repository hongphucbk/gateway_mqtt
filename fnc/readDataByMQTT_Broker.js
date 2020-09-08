const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');
var mqtt = require('mqtt')

let SaveDataToSQLServer = require('./saveDataToSQLServer.js')

let arrAllData = []
module.exports = async function(days, path){
  var mosca = require('mosca');
  var settings = {
    port: 1888,
    //backend: ascoltatore
  };

  var server = new mosca.Server(settings);
  server.on('clientConnected', function(client) {
      log.info('Client connected ', client.id);
  });
  // fired when a message is received

  let _arrData = []
  let json_message;
  let _jsonData;

  server.on('published', function(packet, client) {
    if(packet.topic == process.env.MQTT_TOPIC) 
    {
      try{
        json_message = JSON.parse(packet.payload.toString())
        //console.log(json_message)
        _arrData = []
        json_message.data.forEach(function(tag){
          _jsonData = {
            tag_header : json_message.tag_header,
            site_id : json_message.site_id,
            timestamp: moment(json_message.datetime, "YYYY-MM-DD HH:mm:ss", true), //(data_row.TimeStr),
            tagname: tag.tagname,
            value: parseFloat(tag.value),
            created_at: new Date()
          }
          _arrData.push(_jsonData)
          arrAllData.push(_jsonData)
        })
        //await SaveDataToSQLServer(_arrData)
      }catch(err){
        console.log(err)
        log.error('MQTT error ' + err.message)
      }
  
    }
  });

  let _end;
  let _arrTemp;
  setInterval(async function() {
    RECORD_SAVE_SQL = parseInt(process.env.RECORD_SAVE_SQL)
    _end = arrAllData.length > RECORD_SAVE_SQL ? RECORD_SAVE_SQL : arrAllData.length;
    //console.log('start', arrAllData.length)
    if(_end > 0){
      _arrTemp = arrAllData.slice(0, _end);
      for (var i = 0; i <= _end; i++) {
        arrAllData.pop()
      }

      log.info('MQTT stored ' +  _end + ' records to database, backlog = ' + arrAllData.length)
      //console.log('end', arrAllData.length)
      await SaveDataToSQLServer(_arrTemp)
    }
    
  }, process.env.INTERVAL_SAVE_SQL)
  
}


