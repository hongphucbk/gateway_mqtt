const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')
let deleteDuplicateData = require('./deleteDuplicateData.js')

module.exports = async function(arrData){
  //console.log('data = ', arrData)
  let strDt = '';
  let current = moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
  let _backfill = null; 
  let _strTag_header = null;
  await arrData.forEach(function(objdt){
    if(objdt.backfill == 1){
      _backfill = 1
    }

    if (objdt.tag_header != null) {
      _strTag_header = objdt.tag_header
    }
  	let strTime = moment(objdt.timestamp).format("YYYY-MM-DD HH:mm:ss")
  	strDt = strDt + "('" + objdt.site_id + "', '" + objdt.tagname + "', " 
                  +  objdt.value + ", '" + strTime + "', '"+current + "'," 
                  +  _backfill + ", '" + _strTag_header + "'),"
  })
  strDt = strDt.substr(0, strDt.length - 1);

  let strQuery = 'INSERT INTO ' + process.env.SQL_TABLE_NAME + ' (site_id, tagname, ' 
               + 'datavalue, time_stamp, created_at, backfill, tag_header) '
  						 + ' VALUES ' 
               + strDt
  let strQuery1 = `WITH acte AS (
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
    let pool = await sql.connect(sqlConfig)
    let result1 = await pool.request()
      						.query(strQuery)
    if(_backfill){
      let result2 = await pool.request()
                  .query(strQuery1)
    }

    //deleteDuplicateData(process.env.SQL_TABLE_NAME)
	} catch (err) {
    console.log("SQL Error: " + err.message)
    log.error('SQL has error when stored data: ' + err.message)
  }
}
