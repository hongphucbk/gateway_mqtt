const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');
var mqtt = require('mqtt')

let SaveDataToSQLServer = require('./saveDataToSQLServer.js')

let arrAllData = []
module.exports = async function(days, path){
  let client  = mqtt.connect(process.env.MQTT_URL)
  client.on('connect', function () {
    //console.log('Connect MQTT', ...args)
    client.subscribe('Flexy/Data1', function (err) {
      if (!err) {
        log.info('MQTT is connected to Broker successfully')
        //client.publish('data', 'Hello mqtt')
      }
    })
  })

  client.on('message', async function (topic, payload) { 
    if (topic== process.env.MQTT_TOPIC)
    {
      //console.log(payload.toString())
      try{
        let json_message = JSON.parse(payload.toString())
        //console.log(json_message)
        let _arrData = []
        json_message.data.forEach(function(tag){
          let _jsonData = {
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
        //console.log(arrAllData)
        //console.log(arrAllData.length)
        //await SaveDataToSQLServer(_arrData)
      }catch(err){
        //console.log("Error" + err.message)
        log.error('MQTT error ' + err.message)
      }
    }
  })

  setInterval(async function() {
    RECORD_SAVE_SQL = parseInt(process.env.RECORD_SAVE_SQL)
    let _end = arrAllData.length > RECORD_SAVE_SQL ? RECORD_SAVE_SQL : arrAllData.length;
    
    let _arrTemp = arrAllData.slice(0, _end);
    for (var i = 0; i <= _end; i++) {
      arrAllData.pop()
    }

    if(_end > 0){
      log.info('MQTT stored ' +  _end + ' records to database, backlog = ' + arrAllData.length)
      //console.log('end', arrAllData.length)
      await SaveDataToSQLServer(_arrTemp)
    }
    
  }, process.env.INTERVAL_SAVE_SQL)
}


