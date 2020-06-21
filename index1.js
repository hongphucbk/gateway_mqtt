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
//*******************************************
//joining path of directory

async function run(){
	setInterval(async function(){
  	readFilesFromFlexy();
  	//await writeAckOPCUA()
  	console.log('====================================')
  }, 20000);

  setInterval(async function(){
  	deleteDataAfter10days(strSQLTableName)
  }, 5000);
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
	  //listing all files using forEach
	  await files.forEach(async function (file) {
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
      let processedPath = directoryPath + '\\Processed\\' + moment(new Date()).format("YYYYMMDD-HHmmss") + '_' + file;
      
      if (arrInfo.length !== 6) {
      	console.log('Err! Data format in correct')

      	fs.copyFileSync(currentPath, errPath);
      	fs.unlinkSync(currentPath)

      }else{
      	let site_id = arrInfo[0];
      	let ip = arrInfo[1];
      	let port = parseInt(arrInfo[2]);
      	let tagname = arrInfo[3];

      	

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
							  	console.log('Saved SQL status - ', arrInfo[0],' ', sts)
							  	if (sts == 0) {
							    	fs.copyFileSync(currentPath, errPath);
      							fs.unlinkSync(currentPath)
      							await delay(100);
							    }  
							  	await exportToCSVFile(site_id, tagname, arrExportData)							  	
							    console.log('CSV ' + arrInfo[0] + 'file successfully processed');
							    if (isMoveFile) {
							    	fs.copyFileSync(currentPath, processedPath);
      							fs.unlinkSync(currentPath)
							    }  
      						
							  });
				// await console.log('----end of file----', new Date())
	      let OPCUAstatus = await writeAckOPCUA(site_id, tagname, ip, port);
	      console.log('OPCUAstatus ', site_id,' ', OPCUAstatus)
	      await delay(100);
      }
      
      
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
	   console.log("SQL Error " + err)
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

      let nodeToWrite = {
		    nodeId: process.env.OPCUA_NODEID, //+ tagname,
		    attributeId: AttributeIds.Value,
		    value: {
	        value: {
		        dataType: DataType.Float, 
		        value: 1.0
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