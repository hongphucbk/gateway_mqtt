//requiring path and fs modules
require('dotenv').config()

const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');
const stripBom = require('strip-bom-stream');
var sql = require("mssql");
const sqlConfig = require('./config/sql.js')
const dateFormat = require('dateformat');
const ExportToCsv = require('export-to-csv').ExportToCsv;
const mkdirp = require('mkdirp');
const rimraf = require("rimraf");

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
const SQL_TABLE_STATUS = process.env.SQL_TABLE_STATUS;

const isMoveFile = parseInt(process.env.IS_MOVE_FILE)
const PROCESS_TIME = parseInt(process.env.PROCESS_TIME)*1000;
const REMOVE_TIME = parseInt(process.env.REMOVE_TIME)*1000;
const PROCESSED_STORE = parseInt(process.env.PROCESSED_STORE)*1000;
//*******************************************
//joining path of directory

async function run(){
	setInterval(async function(){
  	readFilesFromFlexy();
    checkConnection()
  	//await writeAckOPCUA()
  	console.log('====================================')
  }, PROCESS_TIME);

  setInterval(async function(){
  	deleteDataAfter10days(strSQLTableName)
    deleteDataAfter10days(SQL_TABLE_STATUS)
    deleteProcessedFolder(PROCESSED_STORE, process.env.CSV_FLEXY_PATH)
  }, REMOVE_TIME);  
}
run();


async function readFilesFromFlexy(){
	
	const directoryPath = process.env.CSV_FLEXY_PATH;
	//passsing directoryPath and callback function
	let inprogressFolder = directoryPath + '\\Inprogress';
	await fs.readdir(inprogressFolder, async function (err, files) {
	  //handling error
	  if (err) {
	    return console.log('Unable to scan directory: ' + err);
	  } 
	  let count = 0;
	  //listing all files using forEach
	  await files.forEach(async function (file) {
	  	count = count +1;
	  	if (count < 10) {
	  	let arrData = []
	  	let arrExportData = [
        {
          TimeStamp: 'TimeStamp',
          Tagname: 'Tagname',
          Value: 'Value',
        }
      ]
      // Do whatever you want to do with the file
      //console.log(file);
      let arrInfo = file.split("_")
      console.log(file);

      let currentPath = inprogressFolder + '\\' + file;
      let errPath = directoryPath + '\\Errors\\' + moment(new Date()).format("YYYYMMDD-HHmmss") + '_' + file;
      let processedPath = directoryPath + '\\Processed\\';
      
      if (arrInfo.length !== 7) {
      	console.log('Err! Data format in correct')

      	fs.copyFileSync(currentPath, errPath);
      	fs.unlinkSync(currentPath)

      }else{
      	let site_id = arrInfo[0];
      	let ip = arrInfo[1];
      	let port = parseInt(arrInfo[2]);
      	let tagname = arrInfo[3];
        let ackTag = arrInfo[4];

      	fs.createReadStream(currentPath)
				  .pipe(csv({separator:';'}))
				  .on('data', (data_row) => {
				  	//console.log('-----------------------------------')
				  	// if (typeof(data_row) == 'Object') {
				  	// 	console.log('object.....')
				  	// }
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
				  })
				  .on('end', async function(){
				  	let sts = await SaveDataToSQLServer(arrData)
				  	console.log('Saved SQL status - ', site_id,' ', sts)
				  	if (sts == 0) {
				    	fs.copyFileSync(currentPath, errPath);
							fs.unlinkSync(currentPath)
							await delay(100);
				    }else{
				    	await exportToCSVFile(site_id, tagname, arrExportData)							  	
					    console.log('CSV ' + site_id + 'file successfully processed');
					    if (isMoveFile) {
							  let _strPath_Date = processedPath + '\\' + moment().format("YYYY_MM_DD")
							 	let strPathFile = _strPath_Date + '\\' + moment(new Date()).format("YYYYMMDD-HHmmss") + '_' + file

							  const folderDate = mkdirp.sync(_strPath_Date);

					    	fs.copyFileSync(currentPath, strPathFile);
  							fs.unlinkSync(currentPath)
					    }

              let OPCUAstatus = await writeAckOPCUA(site_id, ackTag, ip, port);
              console.log('OPC UA status ', site_id,' ', ip + ':' + port ,' ' ,OPCUAstatus)
              await delay(100);
				    }
				  	  
						
				  });
				// await console.log('----end of file----', new Date())
	      
      }
      
      } //End if count
	  });
	});
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
    return 1;
	} catch (err) {
    //console.log("SQL Error " + err)
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
  let strFullPath = process.env.CSV_EXPORT_PATH + '\\Data_' + site_id + '_' + dateTime + '.csv'
  fs.writeFileSync(strFullPath, csvData)

  //for Backup
  let _strPath_Year = process.env.CSV_BACKUP_PATH +'\\' + moment().format("YYYY")
  let _strPath_Month = _strPath_Year + '\\' + moment().format("YYYY_MM")
  let _strPath_Date = _strPath_Month + '\\' + moment().format("YYYY_MM_DD")
  let _strPath_Hour = _strPath_Date + '\\' + moment().format("YYYY_MM_DD_HH")

  const folderYear = mkdirp.sync(_strPath_Year);
  const folderMonth = mkdirp.sync(_strPath_Month);
  const folderDate = mkdirp.sync(_strPath_Date);
  const folderHour = mkdirp.sync(_strPath_Hour);
  let strFullPathBackup = _strPath_Hour + '\\DT_' + site_id + '_' + tagname + '_' + dateTime + '.csv'
  fs.writeFileSync(strFullPathBackup, csvData)
}

function deleteDataAfter10days(tableName){
  // connect to your database
  sql.connect(sqlConfig, function (err) {
    if (err){
      console.log(err);
    } 
    else
    {
      var request = new sql.Request();
      let before10days = moment().subtract(1, 'days');
      let beforeday = new Date(before10days)
      //console.log('data', beforeday)
      request.input('beforeday', sql.DateTimeOffset, beforeday);

      request.query('DELETE FROM ' + tableName + ' WHERE created_at < @beforeday', function(err, recordsets) {  
        if (err) console.log(err); 
      });
    }
  })
  sql.on('error', err => {
    console.log('SQL has issue when delete data ', err )
  })
}

async function writeAckOPCUA(site_id, tagname, ip, port){
  try {
      const options = {
          endpoint_must_exist: false,
      };

      const client = await OPCUAClient.create(options);
      await client.connect('opc.tcp://' + ip +  ':' + port);
      const session = await client.createSession({userName: process.env.OPCUA_USERNAME,password:process.env.OPCUA_PASSWORD});
        // step 3 : browse
      //const browseResult = await session.browse("RootFolder");
  
      // console.log("references of RootFolder :");
      // for(const reference of browseResult.references) {
      //     console.log( "   -> ", reference.browseName.toString());
      // }

      // step 4 : read a variable with readVariableValue
      // const dataValue2 = await session.readVariableValue("ns=1;s=ack");
      // console.log(" value = " , dataValue2.toString());
      let strNodeID = 'ns=' + process.env.OPCUA_NODEID_NS + ';s=ack_' + tagname;
      let nodeToWrite = {
		    nodeId: strNodeID, //+ tagname,
		    attributeId: AttributeIds.Value,
		    value: {
	        value: {
		        dataType: DataType.Boolean, 
		        value: true
	        }
		    }
			}
			let res = await session.write(nodeToWrite);
			//console.log(res);
      //await new Promise((resolve) => setTimeout(resolve, 1000000000));
      //await monitoredItemGroup.terminate();
      //await session.close();
      
      await session.close();
      await client.disconnect();
      if (res._value == 0) {
      	return 1
      	console.log("Done !");
      }
      //readOPCUA1();
  } catch (err) {
      console.log("OPC UA Error ", err.message);
      return 0
  }
};

function deleteProcessedFolder(days, path){
  let beforeNdays = moment().subtract(days + 5, 'days');
  let processedPath = path + '\\Processed';

  let TempDate = beforeNdays
  for(let i = 1; i <= 5; i++ ){
    TempDate = moment(TempDate).add(1, 'd');
    let folderName = moment(TempDate).format("YYYY_MM_DD")
    let strFolderPath = processedPath + '\\' + folderName

    if (fs.existsSync(strFolderPath)) {
      rimraf.sync(strFolderPath);
    }
  }
}

function checkConnection(){
  let arrAllSite = []
  fs.createReadStream('./config/site_information.txt')
    .pipe(csv({separator:';'}))
    .on('data', (data) => {
      arrAllSite.push(data)
      // arrExportData.push(jsonExportData)
    })
    .on('end', async function(){
      //console.log(arrAllSite)
      arrAllSite.forEach(function(site){
        readOPCUA(site.site_id, site.ip, site.port, site.username, site.password)
      })
    })
}

async function readOPCUA(site_id, ip, port, username, password){
  try {
    const options = {
        endpoint_must_exist: false,
    };
    const client = OPCUAClient.create(options);

    client.on("backoff", (retry, delay) => {
        //console.log("Backoff ", retry, " next attempt in ", delay, "ms");
        client.disconnect();
    });

    client.on("connection_lost", () => {
      console.log("Connection lost");
      saveConnectionStatus(site_id, 0)
    });

    client.on("connection_reestablished", () => {
      //  console.log("Connection re-established");
    });

    client.on("connection_failed", () => {
      // console.log("Connection failed");
      // saveConnectionStatus(site_id, 0)
    });
    client.on("start_reconnection", () => {
      //  console.log("Starting reconnection");
      client.disconnect();
    });

    client.on("after_reconnection", (err) => {
      //  console.log("After Reconnection event =>", err);
    });

    await client.connect('opc.tcp://' + ip +  ':' + port);
    const session = await client.createSession({userName: username,password:password});
    
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await session.close();
    await client.disconnect();
    console.log('Connect ' + ip + ':' + port + ' successfully');
    saveConnectionStatus(site_id, 1)
  } catch (err) {
      console.log("Connect Error", err.message);
      saveConnectionStatus(site_id, 0)
  }
}

function saveConnectionStatus(site_id, is_connect){
  // connect to your database
  sql.connect(sqlConfig, function (err) {
    if (err){
      console.log(err);
    } 
    else
    {
      var request = new sql.Request();
      request.input('site_id', sql.VarChar, site_id);
      request.input('is_connect', sql.Bit, is_connect );
      request.input('created_at', sql.DateTimeOffset, new Date());

      let strQuery = 'INSERT INTO '+ process.env.SQL_TABLE_STATUS 
                   + ' (site_id, is_connect, created_at) '
                   + ' VALUES (@site_id, @is_connect, @created_at)'
      request.query(strQuery, function(err, recordsets) {  
        if (err) console.log(err); 
        //console.log(recordsets)
        //sql.close()
      });
   
    }
  })
}

