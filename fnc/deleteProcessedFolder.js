const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');

module.exports = async function(days, path){
  let _beforeNdays = moment().subtract(days + 5, 'days');
  let _processedPath = path + '\\Processed';

  let _TempDate = _beforeNdays
  for(let i = 1; i <= 5; i++ ){
    _TempDate = moment(_TempDate).add(1, 'd');
    let folderName = moment(_TempDate).format("YYYY_MM_DD")
    let strFolderPath = _processedPath + '\\' + folderName

    if (fs.existsSync(strFolderPath)) {
      rimraf.sync(strFolderPath);
      log.warn('Deleted folder ' + folderName + ' in ' + strFolderPath)
    } 
  }
}
