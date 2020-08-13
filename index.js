require('dotenv').config()

const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');
const stripBom = require('strip-bom-stream');
const sql = require("mssql");
const sqlConfig = require('./config/sql.js')
const dateFormat = require('dateformat');
const ExportToCsv = require('export-to-csv').ExportToCsv;
const mkdirp = require('mkdirp');
const rimraf = require("rimraf");
//const log = require('log-to-file');
var db = require("./models/database.js")
var mqtt = require('mqtt')
var ping = require('ping');

const delay = (amount = number) => {
  return new Promise((resolve) => {
    setTimeout(resolve, amount);
  });
}

require('events').EventEmitter.defaultMaxListeners = 200;

const strSQLTableName = process.env.SQL_TABLE_NAME;

const isMoveFile = parseInt(process.env.IS_MOVE_FILE)
const PROCESS_TIME = parseInt(process.env.PROCESS_TIME)*1000;
const BACKFILL_PROCESS_TIME = parseInt(process.env.BACKFILL_PROCESS_TIME)*1000;
//const BACKUP_SQL_TIME = parseInt(process.env.BACKUP_SQL_TIME)*86400000;

const REMOVE_TIME = parseInt(process.env.REMOVE_TIME)*1000;
const CHECK_CONNECT_TIME = parseInt(process.env.CHECK_CONNECT_TIME)*1000;
const SQL_EXPORT_TIME = parseInt(process.env.SQL_EXPORT_TIME)*1000

const DELETE_SQL_DAY = parseInt(process.env.DELETE_SQL_DAY);
const PROCESSED_STORE = parseInt(process.env.PROCESSED_STORE);

const directoryPath = process.env.CSV_FLEXY_PATH;
const inprogressFolder = directoryPath + '\\Inprogress';
const processedPath = directoryPath + '\\Processed';

const log = require('./fnc/log.js')
let SaveDataToSQLServer = require('./fnc/saveDataToSQLServer.js')
let SqlExportToCSVFile = require('./fnc/sqlExportToCSVFile.js')
let deleteProcessedFolder = require('./fnc/deleteProcessedFolder.js')
let deleteDataAfterXXdays = require('./fnc/deleteDataAfterXXdays.js')
let readDataByMQTT = require('./fnc/readDataByMQTT.js')
let saveConnectionStatus = require('./fnc/saveConnectionStatus.js')



//********************************************************************************
// Global variable
let arrAllSites = []
arrAllSites.tagnames = []

//********************************************************************************
// PROGRAM BEGIN
async function run(){
  log.info(' =============== START PROGRAM =============== ')
  await getInitConfig_2() 
  delay(2000);
  
  setInterval(async function(){
    readFilesFromFlexy();
  }, BACKFILL_PROCESS_TIME);

  //Check to Delete backup after xx days folder and SQL
  setInterval(async function(){
    deleteDataAfterXXdays(strSQLTableName, DELETE_SQL_DAY)
    deleteProcessedFolder(PROCESSED_STORE, process.env.CSV_FLEXY_PATH)
  }, REMOVE_TIME);

  //2020-Jul-16: Export data from SQL Server
  setInterval(async function() {
    SqlExportToCSVFile(strSQLTableName)
  }, SQL_EXPORT_TIME )

  readDataByMQTT()
  
  setInterval(async function() {
    checkConnectionPing()
  }, 10000 )  
}
run();

//=========================================================
const filterItems = (arr, query) => {
  return arr.filter(el => el.toLowerCase().indexOf(query.toLowerCase()) !== -1)
}

function getInitConfig_2(){
  try{
    let sql = "SELECT * FROM site_config";
    var params = []
    db.all(sql, params, (err, rows) => {
      if (err) {
        log.error('SQLite error:', err.message )
      }
      arrAllSites = rows;
      //console.log(arrAllSites)

      arrAllSites.forEach(function(site){
        site.tagnames = []
        site.jsonTags = []
        //console.log(site)
        let sql1 = "SELECT * FROM tag_config where site_id = ?";
        let params1 = [site.id]
        db.all(sql1, params1, (err, rows1) => {
          if (err) {
            log.error('SQLite error:', err.message )
          }
          
          for ( row of rows1) 
          { 
            site.tagnames.push(row)
            site.jsonTags.push(row)
          }
          log.info('Site', site )
        });
      })
    });
  }catch(err){
    log.error('Initial error:', err.message )
  }
}

async function readFilesFromFlexy(){
  //passsing directoryPath and callback function
  await fs.readdir(inprogressFolder, async function (err, files) {
    //handling error
    if (err) {
      return console.log('Unable to scan directory: ' + err);
    }
    let count = 0;
    //log('Start -------> ' + , strLogFile );
    log.info('Check file in Inprogress folder at ---> ', moment().format("YYYY-MM-DD HH:mm:ss"))

    for(let i = 0; i < files.length; i++){
      let file = files[i];
      let currentPath = inprogressFolder + '\\' + file;
      
      let errPath = directoryPath + '\\Errors\\' + moment().format("YYYYMMDD-HHmmss") + '_' + file;

      let file_infor = fs.statSync(currentPath)
      let fileSizeInBytes = file_infor["size"]
      //console.log(fileSizeInBytes, typeof(fileSizeInBytes))

      count = count +1;
      if (count < process.env.PROCESS_FILE && fileSizeInBytes > 0) {
        let arrData = []
        let arrExportData = [
          {
            TimeStamp: 'TimeStamp',
            Tagname: 'Tagname',
            Value: 'Value',
          }
        ]
        let arrInfo = file.split("_")
        log.info('Read file name: (' +arrInfo.length + ')-' + file + '- ' + fileSizeInBytes + ' bytes');

        if (arrInfo.length < 5) { 
          try{
            log.error('CSV from Flexy - file format is incorrect lenght. Length = ' + arrInfo.length)
            log.error('File name: ' + file) 
          }catch(err){
            log.error('Error file: ' + err.message) 
            //console.log(err.message)
          }
          try{
            fs.copyFileSync(currentPath, errPath);
            fs.unlinkSync(currentPath)
          }catch(err){
            log.error('Move file to Error folder ' + err.message)
          }
        }else{
          let tag_header = arrInfo[0];
          let site_id = arrInfo[1];
          let tagname = arrInfo[2];

          fs.createReadStream(currentPath)
            .on('error', (err) => {
              log.error('Stream file error ' + err.message);
            })
            .pipe(csv({separator:';'}))
            .on('data', (data_row) => {
              try{
                let jsonData = {
                  tag_header: tag_header,
                  site_id : site_id,
                  timestamp: moment(data_row.TimeStr, "DD/MM/YYYY HH:mm:ss", true), //(data_row.TimeStr),
                  tagname: tagname,
                  value: parseFloat(data_row.Value),
                  created_at: new Date(),
                  backfill: 1
                }
                let strDatetime = dateFormat(jsonData.timestamp, "mm/dd/yyyy HH:MM");
                
                arrData.push(jsonData)
              }catch (err){
                log.warn('Error file when stream ' + err.message);
                log.warn('File: ' + file)
              }
            })
            .on('end', async function(){
              if (arrData.length == 0) {
                try{
                  if( fs.existsSync(currentPath) ){
                    fs.copyFileSync(currentPath, errPath);
                    fs.unlinkSync(currentPath)
                    await delay(20);
                  }
                }catch(err){
                  log.error('Move blank file to error folder has error' + err.message);
                }
              }else{
                let sts = await SaveDataToSQLServer(arrData)
                //console.log('SQL', site_id,':',sts)
                log.info('Backfill - SQL stored ' + site_id + '- ' + tagname);
                await delay(20);

                if (isMoveFile) { //is move if data file is processed ? 1 move, 0 do not move
                  let _strPath_Date = processedPath + '\\' + moment().format("YYYY_MM_DD")
                  let strPathFile = _strPath_Date + '\\' + moment().format("YYYYMMDD-HHmmss") + '_' + file

                  const folderDate = mkdirp.sync(_strPath_Date);
                  try{
                    if( fs.existsSync(currentPath) ){
                      fs.copyFileSync(currentPath, strPathFile);
                      fs.unlinkSync(currentPath)
                      await delay(20);
                    }
                  }catch(err){
                    log.error('Move processed file to Processed folder has error' + err.message);
                  }
                }
              }                   
            }) 
          // await console.log('----end of file----', new Date())  
          await delay(20);
        }
      } //End if count
    } 
  })
}




async function checkConnectionPing(){
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
