const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const dateFormat = require('dateformat');
const fs = require('fs');
const csv = require('csv-parser');
let exportToCSVFileNew = require('./exportToCSVFileNew.js')
let sql_Update_Data = require('./sql_Update_Data.js')
const ExportToCsv = require('export-to-csv').ExportToCsv;

module.exports = async function(tblName){
  let arrExportData = [
                        {
                          TimeStamp: 'TimeStamp',
                          Tagname: 'Tagname',
                          Value: 'Value',
                        }
                      ]
  let sql_id = 1;
  sql.connect(sqlConfig, async function (err) {
    if (err){
      log.error('SQL has error when connect: ', err.message);
    } 
    else
    {
      var request = new sql.Request();
      let beforeXXtime = moment().add(parseInt(process.env.LOCAL_TIME), 'hours').subtract(30, 'seconds');
      //let beforeXXtime = moment().add(parseInt(process.env.LOCAL_TIME), 'hours').subtract(2, 'minutes');
      let beforeday = new Date(beforeXXtime)
      //console.log('data', beforeday)
      request.input('beforeTime', sql.DateTime, beforeday);

      request.query('SELECT TOP ' + process.env.SQL_LINE + '  * FROM ' + tblName + ' WHERE flag IS NULL AND created_at < @beforeTime',async function(err, result) {  
        if (err) log.error('SQL has error when query ', err.message); 
        //console.log('aaa = ', result.recordsets[0].length)
        if (result && result.recordsets[0].length > 0) {
          result.recordsets[0].forEach(function(row){
            let strDatetime = dateFormat(moment(row.time_stamp).subtract( parseInt(process.env.LOCAL_TIME),'hours'), "mm/dd/yyyy HH:MM");
            // console.log('row.time_stamp = ', row.time_stamp)
            // console.log('time = ', strDatetime)
            let jsonExportData = {
              TimeStamp: strDatetime,
              Tagname: row.tag_header + ':'+ row.site_id + '.'+ row.tagname,
              Value: row.datavalue.toFixed(1),
            }
            arrExportData.push(jsonExportData)

            sql_id = row.id
          })
          log.info('SQL Exported to ID ', sql_id)
          await exportToCSVFileNew(arrExportData)
          sql_Update_Data(tblName, sql_id)
        }
        

      });
    }
  })
  sql.on('error', err => {
    //console.log('SQL has error when delete data ', err.message )
    log.error('SQL has error when export data: ' + err.message)
  })

}
