const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery');
/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
 /*
// Pubsub trigger in PRODUCTION
exports.ecommInsertandUpdate = (event, context) => {
  const pubsubMessage = event.data;
  console.log(Buffer.from(pubsubMessage, 'base64').toString());
  insertAndUpdateCsv();
};
*/
//testing purposes we will be using HTTP trigger
exports.ecommInsertAndUpdate = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  res.status(200).send(message);
  insertAndUpdateCsv();
};


async function getRows(datasetId, tableId) {
  
  const bigquery = new BigQuery();

  // List rows in the table
  const [rows] = await bigquery
    .dataset(datasetId)
    .table(tableId)
    .getRows();
      const rows_name_list = []
  console.log('Rows:');
  rows.forEach(row => rows_name_list.push(row['file']));
  return rows_name_list
}

async function listFilesByPrefix(bucketName) {
  // Creates a client
  const storage = new Storage();

  const delimiter = '/';

  const options = {
    //prefix: prefix,
  };

  if (delimiter) {
    options.delimiter = delimiter;
  }

  // Lists files in the bucket, filtered by a prefix
  const [files] = await storage.bucket(bucketName).getFiles(options);
  
  console.log('Files:', files);
  return files
}

async function compareBigQueryAndCloudStorageFiles(datasetId, tableId, bucketName, callback){
  let rows = await getRows(datasetId, tableId)
  console.log('rows in bigquery', rows);
  let files = await listFilesByPrefix(bucketName)
  console.log('files from Google Cloud Storage', files)
  let fileDiff = callback(rows, files);
  return fileDiff;
}

async function getFileDiff(datasetId, tableId, bucketName){
  try {
    return await compareBigQueryAndCloudStorageFiles(datasetId, tableId, bucketName, function(rows, files){  
      const newFiles = []
      files.forEach(file => {
       newFiles.push(file.name);
      });
      
      let diff = newFiles.filter(file => !rows.includes(file))
      console.log('diff', diff)
      return diff
    })
  }catch(error){
    console.log('error in file diff', error);
  }
}

async function insertCSV(){
  const dataset = 'ecomm_production'
  const table = 'transactions'
  const migrationTable = 'migration_files'
  const bucket = 'srichand-ecomm-production'

  let fileList = await getFileDiff(dataset, migrationTable, bucket)
  
  console.log('length', fileList.length)
  console.log('each fileList', fileList)
 
  if (fileList.length === 0){
    console.log('no reports to save');
    return;
  }

  for (let i = 0; i < fileList.length; i++) {
    console.log('counter', i)
    try {
      await sendToBigQuery(fileList[i], dataset, table, bucket);
      await migrationFileToBigQuery(fileList[i], dataset, migrationTable);
    }catch(error){
      console.log('error inserting files', error);
    }
  }
   
}

async function updateCSV(){
  console.log('starting updateCSV');

  const bigquery = new BigQuery();
  const storageClient = new Storage();

  const projectName = 'data-warehouse-srichand'
  const dataset = 'ecomm_production'
  const transactionsTableName = 'transactions'
  const migrationTable = 'migration_files_for_update'
  const bucket = 'srichand-ecomm-production-update'
  let fileList = await getFileDiff(dataset, migrationTable, bucket)
  
  console.log('length', fileList.length)
  console.log('each fileList', fileList)
 
  if (fileList.length === 0){
    console.log('no reports to save');
    return;
  }

  for (let i = 0; i < fileList.length; i++) {
    console.log('counter', i)
    let csvFile = fileList[i];
    let tempTableName = 'temp_table';
    try {
      // parameters: deleteAndAppend(project, datasetId, tempTableId, transactionsTableId, fileName)
      await deleteAndAppend(projectName, dataset, tempTableName, transactionsTableName, csvFile, bucket);
      await migrationFileToBigQuery(csvFile, dataset, migrationTable);
    }catch(error){
      console.log('error when updating files', error);
    }
  }
}

async function insertAndUpdateCsv(){
  await insertCSV();
  await updateCSV();
}

async function migrationFileToBigQuery(reportName, datasetId, tableId){
  const {BigQuery} = require('@google-cloud/bigquery');
  const bigquery = new BigQuery();
    
  if (typeof reportName === 'undefined'){
    console.log('no report to send to big query')
    return;
  }
  async function insertRowsAsStream() {
  const rows = [{file: reportName}];

    // Insert data into a table
  await bigquery
    .dataset(datasetId)
    .table(tableId)
    .insert(rows);

  console.log(`Inserted ${rows.length} row for migration file.`);

  }
  await insertRowsAsStream();
}

async function sendToBigQuery(reportName, datasetId, tableId, bucketName){
  if (typeof reportName === 'undefined'){
    console.log('no report to save')
    return;
  }

  const {BigQuery} = require('@google-cloud/bigquery');
  const {Storage} = require('@google-cloud/storage');

  const filename = reportName;
  // Imports a GCS file into a table with manually defined schema.

  // Instantiate clients
  const bigqueryClient = new BigQuery();
  const storageClient = new Storage();

  const [table] = await bigqueryClient
    .dataset(datasetId)
    .table(tableId)
    .get();

  const destinationTableRef = table.metadata.tableReference;

  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,
    location: 'US',
    schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
    writeDisposition: 'WRITE_APPEND',
    destinationTable: destinationTableRef
  };

  // Load data from a Google Cloud Storage file into the table
  const [job] = await bigqueryClient
    .dataset(datasetId)
    .table(tableId)
    .load(storageClient.bucket(bucketName).file(filename), metadata);

  // load() waits for the job to finish
  console.log(`Job ${job.id} completed.`);

  // Check the job's status for errors
  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }

  return reportName;
  
}

async function deleteAndAppend(project, datasetId, tempTableId, transactionsTableId, fileName, bucketName) {
  const {BigQuery} = require('@google-cloud/bigquery');
  const {Storage} = require('@google-cloud/storage');
  const bigquery = new BigQuery();
  const storageClient = new Storage();

  //** create new table
  const [tableNew] = await bigquery
    .dataset(datasetId)
    .createTable(tempTableId);

  console.log(`New temp table created.`);

  // Retrieve destination table reference
  const [tempTable] = await bigquery
    .dataset(datasetId)
    .table(tempTableId)
    .get();

  const destinationTableRefTemp = tempTable.metadata.tableReference;
  
  const [transactionsTable] = await bigquery
    .dataset(datasetId)
    .table(transactionsTableId)
    .get();

  const destinationTableRefTransactions = transactionsTable.metadata.tableReference;

  // Set load job options
  const options = {
    schema: transactionsTable.metadata.schema,
    schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
    skipLeadingRows: 1,
    writeDisposition: 'WRITE_APPEND',
    createDisposition: 'CREATE_IF_NEEDED',
    destinationTable: destinationTableRefTemp,
  };
      
  // Load data from a google cloud storage into the table
  const [jobLoad] = await bigquery
    .dataset(datasetId)
    .table(tempTableId)
    .load(storageClient.bucket(bucketName).file(fileName), options);
    //.load(fileName, options);

  console.log(`Job ${jobLoad.id} load csv to table completed.`);

  // Check the job's status for errors
  const errors = jobLoad.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }

  //** Delete matching line_items from original table
  const queryDelete = `DELETE \`${project}.${datasetId}.${transactionsTableId}\` i
              WHERE i.line_item_id IN (SELECT line_item_id from \`${project}.${datasetId}.${tempTableId}\`)`;

  // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
  const optionsDelete = {
    query: queryDelete,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'US',
  };

  // Run the query as a job
  const [jobDelete] = await bigquery.createQueryJob(optionsDelete);
  console.log(`Job ${jobDelete.id} started.`);

  // Wait for the query to finish
  const [rows] = await jobDelete.getQueryResults();
  console.log(`Job ${jobDelete.id} deleted matching from original completed.`);

      //** append new table to original table
  const queryAppend = `Select * from \`${project}.${datasetId}.${tempTableId}\``;

  // Set load job options
  const optionsAppend = {
    query: queryAppend,
    schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
    skipLeadingRows: 1,
    writeDisposition: 'WRITE_APPEND',
    destinationTable: destinationTableRefTransactions,
  };
      
  //const [jobAppend] = await bigquery.createQueryJob(optionsAppend);
  const [jobAppend] = await jobAppendFunc(optionsAppend); 
  console.log(`Job ${jobAppend.id} started.`);

  // Wait for the query to finish
  const [rows3] = await jobAppend.getQueryResults();
  console.log(`Job ${jobAppend.id} adding rows to original completed.`);
      
  //** Delete table
  await bigquery
    .dataset(datasetId)
    .table(tempTableId)
    .delete();

  console.log(`Table ${tempTableId} deleted.`);

}

async function jobAppendFunc(options){
  const {BigQuery} = require('@google-cloud/bigquery');
  const bigquery = new BigQuery();

  return await bigquery.createQueryJob(options)
}
