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
const CHECK_CONNECT_TIME = parseInt(process.env.CHECK_CONNECT_TIME)*1000;
const BACKUP_SQL_DAY = parseInt(process.env.BACKUP_SQL_DAY);
const directoryPath = process.env.CSV_FLEXY_PATH;
const inprogressFolder = directoryPath + '\\Inprogress';

//*******************************************
//joining path of directory

async function run(){
	setInterval(async function(){
  	await writeAckOPCUA('MY999952','Temperature','10.137.16.1', 4840)
    readOPCUA('MY999952', '10.137.16.1', 4840, 'user1', 'password1')
  	console.log('====================================')
  }, PROCESS_TIME);
}
run();


 

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
      writeConnectionToCSV(site_id, 0)
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
  } catch (err) {
      console.log("Connect Error", err.message);

  }
}

