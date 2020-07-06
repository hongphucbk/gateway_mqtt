//requiring path and fs modules
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

const  { 
  OPCUAClient,
  resolveNodeId, 
  DataType,
  AttributeIds,
  ClientMonitoredItemGroup, 
  TimestampsToReturn
 } = require("node-opcua-client");
const opcua = require("node-opcua");

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


const REMOVE_TIME = parseInt(process.env.REMOVE_TIME)*1000;
const CHECK_CONNECT_TIME = parseInt(process.env.CHECK_CONNECT_TIME)*1000;

const BACKUP_SQL_DAY = parseInt(process.env.BACKUP_SQL_DAY);
const PROCESSED_STORE = parseInt(process.env.PROCESSED_STORE);

const directoryPath = process.env.CSV_FLEXY_PATH;
const inprogressFolder = directoryPath + '\\Inprogress';
const processedPath = directoryPath + '\\Processed';
const logFolder = directoryPath + '\\Log\\';
mkdirp.sync(logFolder);
//********************************************************************************
// Global variable
let arrAllSites = []

const opts = {
    // errorEventName:'error',
        logDirectory:logFolder, // NOTE: folder must exist and be writable...
        fileNamePattern:'log-<DATE>.log',
        dateFormat:'YYYY.MM.DD HH'
};
const log = require('simple-node-logger').createRollingFileLogger( opts );
//********************************************************************************
// PROGRAM BEGIN
async function run(){
  log.info(' =============== START PROGRAM =============== ')
  console.log(' =============== START PROGRAM =============== ')
  await getInitConfig()
  delay(1000);
  
	setInterval(async function(){
    strLogPath = logFolder + '\\' + moment(new Date()).format("YYYYMMDD");
  	readFilesFromFlexy();
  	//console.log('============================================================')
  }, BACKFILL_PROCESS_TIME);

  //Check to Delete backup after xx days
  setInterval(async function(){
  	deleteDataAfterXXdays(strSQLTableName, BACKUP_SQL_DAY)
    deleteProcessedFolder(PROCESSED_STORE, process.env.CSV_FLEXY_PATH)
  }, REMOVE_TIME);  

  setInterval(async function(){
    //checkConnection()
  }, CHECK_CONNECT_TIME);

  setInterval(async function(){
    readDataFromFlexy()
  }, PROCESS_TIME);
}
run();

const filterItems = (arr, query) => {
  return arr.filter(el => el.toLowerCase().indexOf(query.toLowerCase()) !== -1)
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
    log.info('Read file at ---> ', moment().format("YYYY-MM-DD HH:mm:ss"))

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
        
        // Do whatever you want to do with the file
        let arrInfo = file.split("_")
        //console.log('File name: ' +arrInfo.length + ' - ' + file);
        log.info('Read file name: (' +arrInfo.length + ')-' + file + '- ' + fileSizeInBytes + ' bytes');

        if (arrInfo.length !== 7) { 
          try{
            log.error('CSV from Flexy - file format is incorrect lenght. Length = ' + arrInfo.length)
            log.error('File name: ' + file) 
          }catch(err){
            console.log(err.message)
          }

          try{
            fs.copyFileSync(currentPath, errPath);
            fs.unlinkSync(currentPath)
          }catch(err){
            log.error('Move file to Error folder.' + err.message)
          }
        }else{
        	let site_id = arrInfo[0];
        	let ip = arrInfo[1];
        	let port = parseInt(arrInfo[2]);
        	let tagname = arrInfo[3];
          let ackTag = arrInfo[4];

        	fs.createReadStream(currentPath)
            .on('error', (err) => {
              //console.log('Stream file error')
              log.error('Stream file error ' + err.message);
            })
  				  .pipe(csv({separator:';'}))
  				  .on('data', (data_row) => {
              try{
                let jsonData = {
                  site_id : site_id,
                  ip: ip,
                  timestamp: moment(data_row.TimeStr, "DD/MM/YYYY HH:mm:ss", true), //(data_row.TimeStr),
                  tagname: tagname,
                  value: parseFloat(data_row.Value),
                  created_at: new Date(),
                }
                let strDatetime = dateFormat(jsonData.timestamp, "mm/dd/yyyy HH:MM");
                let jsonExportData = {
                  TimeStamp: strDatetime,
                  Tagname: site_id + ':METTUBE.'+ tagname,
                  Value: jsonData.value.toFixed(1),
                }
                arrData.push(jsonData)
                arrExportData.push(jsonExportData)
              }catch (err){
                //console.log('Error file when stream ' + err.message)
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
                    await delay(50);
                  }
                }catch(err){
                  log.error('Move blank file to error folder has error' + err.message);
                }
              }else{
                let sts = await SaveDataToSQLServer(arrData)
                //console.log('SQL', site_id,':',sts)
                log.info('SQL saved ' + site_id + '- ' + tagname + ': ' + sts);
                await delay(50);

                if (process.env.IS_EXPORT_TO_CSV == 1) {
                  await exportToCSVFile(site_id, tagname, arrExportData)                  
                  log.info('Export CSV ' + site_id + ' ' + tagname + ' file processed successfully');
                }

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
                    //console.log('Move processed file err ' + err.message)
                    //log('Move processed file err ' + err.message, strLogPath + '\\log.txt');
                    log.error('Move processed file to Processed folder has error' + err.message);
                  }
                }
              }			  	  				
  				  }) 
            
  				// await console.log('----end of file----', new Date())  
          await delay(50);
          
        }
      } //End if count
	  } 
	})
}

async function sendAckToFlexy(site_id, ackTag, ip, port){
  //let OPCUAstatus = await writeAckOPCUA(site_id, ackTag, ip, port);
  //console.log('OPC UA', site_id,' ', ackTag + ':',OPCUAstatus)
  //log.info(site_id + ' send ack ' + ackTag + ' to flexy by OPC UA ', OPCUAstatus);
}

async function SaveDataToSQLServer(arrData){
  //console.log('data = ', arrData)
  let strDt = '';
  let current = moment(new Date()).format("YYYY-MM-DD HH:mm:ss")

  await arrData.forEach(function(objdt){ 	
  	let strTime = moment(objdt.timestamp).format("YYYY-MM-DD HH:mm:ss")
  	strDt = strDt + "('" + objdt.site_id + "', '" + objdt.tagname + "', " +  objdt.value + ", '" + strTime + "', '"+current+"' ),"
  })
  strDt = strDt.substr(0, strDt.length - 1);

  //console.log('a =', strDt)

  let strQuery = 'INSERT INTO ' + strSQLTableName + ' (site_id, tagname, datavalue, time_stamp, created_at) '
  						 + ' VALUES ' 
               + strDt
  try {
    let pool = await sql.connect(sqlConfig)
    let result1 = await pool.request()
      						.query(strQuery)
        
    //console.log(result1)
    deleteDuplicateData(strSQLTableName)
    return 1;
	} catch (err) {
    //console.log("SQL Error " + err)
    log.error('SQL has error when stored data:' + err.message)
    return 0;
	}
}

function exportToCSVFile(site_id, tagname, data){
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
  let strFullPath = process.env.CSV_EXPORT_PATH + '\\DT_' + site_id + '_'  + tagname + '_' + dateTime + '.csv'
  
  try{
    fs.writeFileSync(strFullPath, csvData)
    //fs.writeFileSync(strFullPathBackup, csvData)
  }catch (err){
    //console.log('Write CSV have issue ' + err.message)
    log.error('Write CSV have issue: ' + err.message)
  }

  //for Backup
  let _strPath_Year = process.env.CSV_BACKUP_PATH +'\\' + moment().format("YYYY")
  let _strPath_Month = _strPath_Year + '\\' + moment().format("YYYY_MM")
  let _strPath_Date = _strPath_Month + '\\' + moment().format("YYYY_MM_DD")
  let _strPath_Hour = _strPath_Date + '\\' + moment().format("YYYY_MM_DD_HH")

  mkdirp.sync(_strPath_Year);
  mkdirp.sync(_strPath_Month);
  mkdirp.sync(_strPath_Date);
  mkdirp.sync(_strPath_Hour);
  let strFullPathBackup = _strPath_Hour + '\\DT_' + site_id + '_' + tagname + '_' + dateTime + '.csv'
  try{
    fs.writeFileSync(strFullPathBackup, csvData)
  }catch (err){
    console.log('Write CSV have issue ' + err.message)
    log.error('Write CSV ' + site_id + ' - ' + tagname + ' have error: ' + err.message)
  }
}

function deleteDataAfterXXdays(tableName, days){
  sql.connect(sqlConfig, function (err) {
    if (err){
      console.log(err);
    } 
    else
    {
      var request = new sql.Request();
      let beforeXXdays = moment().subtract(BACKUP_SQL_DAY, 'days');
      let beforeday = new Date(beforeXXdays)
      //console.log('data', beforeday)
      request.input('beforeday', sql.DateTimeOffset, beforeday);

      request.query('DELETE FROM ' + tableName + ' WHERE created_at < @beforeday', function(err, recordsets) {  
        if (err) console.log(err); 
      });
    }
  })
  sql.on('error', err => {
    //console.log('SQL has error when delete data ', err.message )
    log.error('SQL has error when delete data: ' + err.message)
  })
}


function deleteProcessedFolder(days, path){
  let beforeNdays = moment().subtract(days + 5, 'days');
  let _processedPath = path + '\\Processed';

  let TempDate = beforeNdays
  for(let i = 1; i <= 5; i++ ){
    TempDate = moment(TempDate).add(1, 'd');
    let folderName = moment(TempDate).format("YYYY_MM_DD")
    let strFolderPath = _processedPath + '\\' + folderName

    if (fs.existsSync(strFolderPath)) {
      rimraf.sync(strFolderPath);
      log.warn('Deleted folder ' + folderName + ' in ' + strFolderPath)
    } 
  }
}

function getInitConfig(){
  fs.createReadStream('./config/site_information.txt')
    .pipe(csv({separator:';'}))
    .on('data', (data) => {
      arrAllSites.push(data) 
      // arrExportData.push(jsonExportData)
    })
    .on('end', async function(){
      arrAllSites.forEach(function(site){
        try{
          site.tagnames = site.tagnames.split('*')
          let arrTemp = []
          for ( tn of site.tagnames) 
          { 
            let _temp = JSON.parse(tn)
            arrTemp.push(_temp)
          }
          site.jsonTags = arrTemp 
          console.log(site)
        }catch(err){
          console.log(err)
          log.error('Convert tagname has error: ' + err.message)
        }
      })
    })
}

async function readDataFromFlexy(){
  const connectionStrategy = {
    initialDelay: 1000,
    maxRetry: 1
  }
  const options = {
      // applicationName: "MyClient",
      connectionStrategy: connectionStrategy,
      // securityMode: MessageSecurityMode.None,
      // securityPolicy: SecurityPolicy.None,
      endpoint_must_exist: false,
  };

  arrAllSites.forEach(async function(site){
    try{
      let _arrData = []
      const client = OPCUAClient.create(options);
      const endpointUrl = 'opc.tcp://' + site.ip +':'+ site.port;

      client.on("backoff", (retry, delay) => {
        //console.log("Backoff ", retry, " next attempt in ", delay, "ms");
        client.disconnect();
      });

      client.on("connection_failed", () => {
        client.disconnect();
        //console.log("Connection failed");
        log.error("OPC UA -" + site.site_id + 'can not connect to Flexy')
        saveConnectionStatus(site.site_id, 0)
        writeConnectionToCSV(site.site_id, 0)
      });


      // step 1 : connect to
      await client.connect(endpointUrl);
      //console.log("OPC UA connected !");
      log.info('OPC UA - '+ site.site_id + ' connected to Flexy')
      saveConnectionStatus(site.site_id, 1)
      writeConnectionToCSV(site.site_id, 1)
      // Step 2 : createSession
      const session = await client.createSession({userName: site.username,password:site.password});
      // console.log("Session created !");

      // Step 4 : read a variable with readVariableValue
      //await site.tagnames.forEach(async function(tagname){
      for await (tag of site.jsonTags) {
        const dataValue2 = await session.readVariableValue("ns="+ site.namespace +";s=" + tag.name);
        //console.log(" value = " , tagname, dataValue2.value.value);

        let _jsonData = {
                  site_id : site.site_id,
                  ip: site.ip,
                  timestamp: new Date(),
                  tagname: tag.sys,
                  value: parseFloat(dataValue2.value.value),
                  created_at: new Date(),
                }
        _arrData.push(_jsonData)
      }
      
      //await console.log(_arrData)
      let _isStoredSucess = await SaveDataToSQLServer(_arrData)
      if (_isStoredSucess) {
        log.info('SQL stored realtime data successfully')
      }else{
        log.error('SQL stored realtime data ERROR')
      }

      // close session
      await session.close();
      // disconnecting
      await client.disconnect();
      log.info('OPC UA - '+ site.site_id + ' disconnect to Flexy')

    }catch(err){
      log.error('OPC UA has error: ' + err.message)
    }
  })

}

function saveConnectionStatus(site_id, is_connect){
    let TempData = [{
                site_id : site_id,
                timestamp: new Date(), //(data_row.TimeStr),
                tagname: 'COMMUNICATION',
                value: parseFloat(is_connect),
                created_at: new Date(),
              }]
    SaveDataToSQLServer(TempData)
}

async function writeConnectionToCSV(site_id, value){
  if (parseInt(process.env.IS_EXPORT_TO_CSV) == 1) {
    let jsonConnectExportData = [
    {
      TimeStamp: 'TimeStamp',
      Tagname: 'Tagname',
      Value: 'Value',
    },
    {
      TimeStamp: dateFormat(new Date(), "mm/dd/yyyy HH:MM"),
      Tagname: site_id + ':METTUBE.'+ 'COMMUNICATION',
      Value: value,
    }]
    //console.log(jsonConnectExportData)
    await exportToCSVFile(site_id, 'COMMUNICATION', jsonConnectExportData)
  }
}

function deleteDuplicateData(tableName){
  // connect to your database
  sql.connect(sqlConfig, function (err) {
    if (err){
      console.log(err);
    } 
    else
    {
      var request = new sql.Request();
      let strQuery = `WITH acte AS (
                        SELECT 
                            *, 
                            ROW_NUMBER() OVER (
                                PARTITION BY 
                                    site_id, 
                                    tagname, 
                                    datavalue,
                                    time_stamp
                                ORDER BY 
                                    site_id, 
                                    tagname, 
                                    datavalue
                            ) row_num
                         FROM ` + tableName + `
                      WHERE  time_stamp < DATEADD(HOUR, 1, GETDATE())
                           AND time_stamp > DATEADD(HOUR, -2, GETDATE())
                         )
                      DELETE FROM acte
                      WHERE row_num > 1;
                      `
      request.query(strQuery, function(err, recordsets) {  
        if (err) console.log(err); 
      });
    }
  })
  sql.on('error', err => {
    //console.log(' )
    log.error('SQL has error when trigger to delete duplicate data: ', err.message)
  })
}