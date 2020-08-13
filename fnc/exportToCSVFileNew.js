const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const dateFormat = require('dateformat');
const fs = require('fs');
const csv = require('csv-parser');
const ExportToCsv = require('export-to-csv').ExportToCsv;

module.exports = async function(data){
  const options = { 
    fieldSeparator: ',',
    quoteStrings: '',
    decimalSeparator: '.',
    showLabels: true, 
    showTitle: false,
    title: '[Data]',
    useTextFile: false,
    useBom: false,
    useKeysAsHeaders: false,
    headers: ['[DATA]'] //<-- Won't work with useKeysAsHeaders present!
  };
  const csvExporter = new ExportToCsv(options);
  const csvData = csvExporter.generateCsv(data, true);
  var dateTime = new Date();
  dateTime = moment(dateTime).format("YYYYMMDD_HHmmss");
  let strFullPath = process.env.CSV_EXPORT_PATH + '\\DT_SQL_' + dateTime + '.csv'
  
  try{
    fs.writeFileSync(strFullPath, csvData)
    //fs.writeFileSync(strFullPathBackup, csvData)
  }catch (err){
    //console.log('Write CSV have issue ' + err.message)
    log.error('Write CSV have issue: ' + err.message)
  }

  if (parseInt(process.env.IS_BACKUP_CSV) > 0) {
    //for Backup
    let _strPath_Year = process.env.CSV_BACKUP_PATH +'\\' + moment().format("YYYY")
    let _strPath_Month = _strPath_Year + '\\' + moment().format("YYYY_MM")
    let _strPath_Date = _strPath_Month + '\\' + moment().format("YYYY_MM_DD")
    let _strPath_Hour = _strPath_Date + '\\' + moment().format("YYYY_MM_DD_HH")

    mkdirp.sync(_strPath_Year);
    mkdirp.sync(_strPath_Month);
    mkdirp.sync(_strPath_Date);
    mkdirp.sync(_strPath_Hour);
    let strFullPathBackup = _strPath_Hour + '\\DT_SQL_' + dateTime + '.csv'
    try{
      fs.writeFileSync(strFullPathBackup, csvData)
      log.info('SQL export to Backup CSV file successfully.')
    }catch (err){
      //console.log('Write CSV have issue ' + err.message)
      log.error('SQL export to backup CSV have error: ' + err.message)
    }
  }
  


}
