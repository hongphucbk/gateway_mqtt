const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
let deleteDuplicateData = require('./deleteDuplicateData.js')
let pool = null;

let strDt = '';
let current;
let _backfill = null;
let _strTag_header = null;
let strTime;
let strQuery;
let strQuery1;

module.exports = async function(arrData){
  current = moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
  _backfill = null;
  _strTag_header = null;
  strDt = '';

  await arrData.forEach(function(objdt){
    if(objdt.backfill == 1){
      _backfill = 1
    }

    if (objdt.tag_header != null) {
      _strTag_header = objdt.tag_header
    }

  	strTime = moment(objdt.timestamp).format("YYYY-MM-DD HH:mm:ss")
  	strDt = strDt + "('" + objdt.site_id + "', '" + objdt.tagname + "', " 
                  +  objdt.value + ", '" + strTime + "', '"+ current + "'," 
                  +  _backfill + ", '" + _strTag_header + "'),"
  })
  strDt = strDt.substr(0, strDt.length - 1);

  strQuery = 'INSERT INTO ' + process.env.SQL_TABLE_NAME + ' (site_id, tagname, ' 
           + 'datavalue, time_stamp, created_at, backfill, tag_header) '
					 + ' VALUES ' 
           + strDt
  //console.log(strQuery)
  strQuery1 = ` WITH acte AS (
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
                       FROM ` + process.env.SQL_TABLE_NAME + `
                    WHERE  created_at < DATEADD(HOUR, 1, GETDATE())
                         AND created_at > DATEADD(HOUR, -2, GETDATE())
                       )
                    DELETE FROM acte
                    WHERE row_num > 1;
                  `

  try {
    if (!pool) {
      pool = await sql.connect(sqlConfig)      
    }else{
      if(!pool._connected){
        pool = await sql.connect(sqlConfig)
        console.log('---Hihi----------------> check connect')
      }else{
        console.log('---Hihi---------------->')
      }

    }
    await pool.query(strQuery)
    if(_backfill){
      await pool.query(strQuery1)
    }
	} catch (err) {
    console.log(err)
    log.error('SQL has error when stored data: ' + err.message)
  }
}
