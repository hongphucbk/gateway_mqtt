const moment = require('moment');
const sql = require("mssql");
const sqlConfig = require('../config/sql.js')
const log = require('./log.js')

module.exports = async function(tableName){
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
                      WHERE  created_at < DATEADD(HOUR, 1, GETDATE())
                           AND created_at > DATEADD(HOUR, -2, GETDATE())
                         )
                      DELETE FROM acte
                      WHERE row_num > 1;
                      `
      request.query(strQuery, function(err, recordsets) {  
        if (err) console.log(err);
        sql.close()
      });
    }
  })
  sql.on('error', err => {
    console.log('SQL has error when trigger to delete duplicate data: ', err.message )
    log.error('SQL has error when trigger to delete duplicate data: ', err.message)
  })
}
