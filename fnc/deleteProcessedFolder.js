const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const fs = require('fs');
const rimraf = require("rimraf");

let _beforeNdays
let _processedPath
let _TempDate
let folderName
let strFolderPath

module.exports = async function(days, path){
  _beforeNdays = moment().subtract(days + 5, 'days');
  _processedPath = path + '\\Processed';

  _TempDate = _beforeNdays
  for(let i = 1; i <= 5; i++ ){
    _TempDate = moment(_TempDate).add(1, 'd');
    folderName = moment(_TempDate).format("YYYY_MM_DD")
    strFolderPath = _processedPath + '\\' + folderName

    if (fs.existsSync(strFolderPath)) {
      rimraf.sync(strFolderPath);
      log.warn('Deleted folder ' + folderName + ' in ' + strFolderPath)
    } 
  }
}
