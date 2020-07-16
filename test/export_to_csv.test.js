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
  let strFullPath = process.env.CSV_EXPORT_PATH + '\\DT_' + site_id + '_'  + tagname + '_' + dateTime + '.csv'
  
  try{
    fs.writeFileSync(strFullPath, csvData)
    //fs.writeFileSync(strFullPathBackup, csvData)
  }catch (err){
    //console.log('Write CSV have issue ' + err.message)
    log.error('Write CSV have issue: ' + err.message)
  }

  //for Backup
  let _strPath_Year = process.env.CSV_BACKUP_PATH +'\\' + moment().format("YYYY")
  let _strPath_Month = _strPath_Year + '\\' + moment().format("YYYY_MM")
  let _strPath_Date = _strPath_Month + '\\' + moment().format("YYYY_MM_DD")
  let _strPath_Hour = _strPath_Date + '\\' + moment().format("YYYY_MM_DD_HH")

  mkdirp.sync(_strPath_Year);
  mkdirp.sync(_strPath_Month);
  mkdirp.sync(_strPath_Date);
  mkdirp.sync(_strPath_Hour);
  let strFullPathBackup = _strPath_Hour + '\\DT_' + site_id + '_' + tagname + '_' + dateTime + '.csv'
  try{
    fs.writeFileSync(strFullPathBackup, csvData)
  }catch (err){
    console.log('Write CSV have issue ' + err.message)
    log.error('Write CSV ' + site_id + ' - ' + tagname + ' have error: ' + err.message)
  }
}
