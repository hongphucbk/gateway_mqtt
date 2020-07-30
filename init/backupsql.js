require('dotenv').config()

const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');
var sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const dateFormat = require('dateformat');
const ExportToCsv = require('export-to-csv').ExportToCsv;
const mkdirp = require('mkdirp');
//--------------------------------------------------------
let strFullPath = process.env.CSV_FLEXY_PATH;
let SQL_TABLE_STATUS = process.env.SQL_TABLE_STATUS

const Backup = mkdirp.sync(strFullPath + '\\Backup\\');
let fileName = strFullPath  + '\\Backup\\SQL_DB_Backup_' + moment().format("YYYYMMDD_HHmmss")+ '.bak'

let strQuery = 'BACKUP DATABASE '+ process.env.SQL_SERVER_DATABASE + " TO DISK = '" + fileName + "'"
			console.log(strQuery)
my_query(strQuery)

async function my_query(strQuery){           
  try {
    let pool = await sql.connect(sqlConfig)
    let result1 = await pool.request()
      						.query(strQuery)
        
    console.log(result1)
    
	} catch (err) {
	   console.log("SQL Error " + err)
	}
}
