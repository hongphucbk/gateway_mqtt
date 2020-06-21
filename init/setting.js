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
let strTableName = process.env.SQL_TABLE_NAME

const Inprogress = mkdirp.sync(strFullPath + '\\Inprogress');
const Errors = mkdirp.sync(strFullPath + '\\Errors');
const Processed = mkdirp.sync(strFullPath + '\\Processed');

let strQuery = 'CREATE TABLE ' + strTableName
			 +	'( id BIGINT IDENTITY PRIMARY KEY,'
			 +	'  site_id VARCHAR(60) NOT NULL,'
			 +'	  tagname VARCHAR(60) NOT NULL,'
			 +'  datavalue FLOAT,'
			 +' time_stamp DATETIME,'
			 +'  created_at DATETIME,'
			 +'  flag float null,'
			 +'  note VARCHAR(50) null'
			 +')' 
createDatabase(strTableName, strQuery)

async function createDatabase(table, strQuery){           
  try {
    let pool = await sql.connect(sqlConfig)
    let result1 = await pool.request()
      						.query(strQuery)
        
    console.log(result1)
    
	} catch (err) {
	   console.log("SQL Error " + err)
	}
}
