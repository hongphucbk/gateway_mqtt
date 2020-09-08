const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
const dateFormat = require('dateformat');
const fs = require('fs');
const csv = require('csv-parser');

let strQuery
let pool
module.exports = async function(tblName, sql_id){
  let strQuery = 'UPDATE ' + tblName + ' SET flag = 1 WHERE flag IS NULL AND id <= @sql_id'
  try {
    if (!pool) {
      pool = await sql.connect(sqlConfig)      
    }else{
      if(!pool._connected){
        pool = await sql.connect(sqlConfig)
        console.log('---Hihi 3----------------> check connect')
      }else{
        //console.log('---Hihi 2---------------->')
      }

    }
    let result1 = await pool.request()
                            .input('sql_id', sql.Int, sql_id)
                            .query(strQuery)
    if(result1){
      pool.close()
    }
            
  } catch (err) {
    //console.log("SQL export data " + err)
    log.error('SQL update data:' + err.message)
    //return 0;
  }

    
}
