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
let strTableName = process.env.SQL_TABLE_NAME

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
                      WHERE  time_stamp < DATEADD(DAY, 1, GETDATE())
                           AND time_stamp > DATEADD(DAY, -10, GETDATE())
                         )
                      DELETE FROM acte
                      WHERE row_num > 1;
                      `
      request.query(strQuery, function(err, recordsets) {  
        if (err) console.log(err); 
        console.log('Delete all duplicate')
      });
    }
  })
  sql.on('error', err => {
    console.log(' SQL has error when trigger to delete duplicate data: ', err.message)
    //log.error('SQL has error when trigger to delete duplicate data: ', err.message)
  })
}

deleteDuplicateData(strTableName)
