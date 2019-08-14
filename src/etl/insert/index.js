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
exports.helloPubSub = (event, context) => {
  const pubsubMessage = event.data;
  console.log(Buffer.from(pubsubMessage, 'base64').toString());
  //five();
  six();
  //loadCSVFromGCS();
};
*/

//testing purposes we will be using HTTP trigger
exports.ecommInsertTest = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  res.status(200).send(message);
  six();
};


async function getRows(datasetId, tableId) {
  // [START bigquery_browse_table]

  // Import the Google Cloud client library and create a client
  
  const bigquery = new BigQuery();

  //async function browseRows() {
    // Displays rows from "my_table" in "my_dataset".

    /**
     * TODO(developer): Uncomment the following lines before running the sample.
     */
//     const datasetId = datasetId;
//     const tableId = tableId;

    // List rows in the table
    const [rows] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .getRows();
	const rows_name_list = []
    console.log('Rows:');
    rows.forEach(row => rows_name_list.push(row['file']));
    return rows_name_list
  //}
  // [END bigquery_browse_table]
  //await browseRows();
}

async function listFilesByPrefix(bucketName) {
  // [START storage_list_files_with_prefix]
  // Imports the Google Cloud client library
  

  // Creates a client
  const storage = new Storage();

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  //const bucketName = bucketName;
  //const prefix = 'Prefix by which to filter, e.g. public/';
  const delimiter = '/';

  const options = {
    //prefix: prefix,
  };

  if (delimiter) {
    options.delimiter = delimiter;
  }

  // Lists files in the bucket, filtered by a prefix
  const [files] = await storage.bucket(bucketName).getFiles(options);
  //const [files] = await storage.bucket(bucketName).getFiles();
  
  console.log('Files:', files);
  return files
  // [END storage_list_files_with_prefix]
}

async function getOneFile(datasetId, tableId, bucketName, callback){
  let rows = await getRows(datasetId, tableId)
  console.log('rows in bigquery', rows);
  let files = await listFilesByPrefix(bucketName)
  console.log('files from Google Cloud Storage', files)
  let f = callback(rows, files);
  return f;
}

async function two(datasetId, tableId, bucketName){
  return await getOneFile(datasetId, tableId, bucketName, function(rows, files){  
    const newFiles = []
  	console.log('in two');
  	files.forEach(file => {
   	 newFiles.push(file.name);
  	});
    
    console.log('new files in two', newFiles)
    let diff = newFiles.filter(file => !rows.includes(file))
    console.log('diff', diff)
    return diff
  })
}

async function six(){
  const dataset = 'ecomm_test'
  const table = 'transactions'
  const migrationTable = 'migration_files'
  const bucket = 'srichand-ecomm-staging'
  let reportNames = await two(dataset, migrationTable, bucket)
  
   console.log('length', reportNames.length)
   console.log('each reportNames', reportNames)
 
   if (reportNames.length === 0){
     console.log('no reports to save');
     return;
   }
   //For production will use all diffs
//   for (let i = 0; i < reportNames.length; i++) {
//     console.log('counter', i)
//     let m = await sendToBigQuery(reportNames[i])
//     let n = migrationFileToBigQuery(reportNames[i])
//   }

// testing will use only 2 at a time
   for (let i = 0; i < 1; i++) {
     console.log('counter', i)
     let m = await sendToBigQuery(reportNames[i], dataset, table, bucket)
     let n = await migrationFileToBigQuery(reportNames[i], dataset, migrationTable)
   }
   
}

async function migrationFileToBigQuery(reportName, datasetId, tableId){
  	const {BigQuery} = require('@google-cloud/bigquery');
  	const bigquery = new BigQuery();
    
    if (typeof reportName === 'undefined'){
      console.log('no report to send to big query')
      return;
    }
  async function insertRowsAsStream() {
//     const datasetId = datasetId;
//     const tableId = tableId;
     const rows = [{file: reportName}];

    // Insert data into a table
    await bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
  }
  // [END bigquery_table_insert_rows]
  await insertRowsAsStream();
}

async function sendToBigQuery(reportName, datasetId, tableId, bucketName){
    if (typeof reportName === 'undefined'){
      console.log('no report to save')
      return;
  	}
    
     const {BigQuery} = require('@google-cloud/bigquery');
     const {Storage} = require('@google-cloud/storage');

//     const datasetId = datasetId;
//     const tableId = tableId;

/**
 * This sample loads the CSV file at
 * https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.csv
 *
 * TODO(developer): Replace the following lines with the path to your file
 */
      //const bucketName = bucketName;
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
        location: 'asia-east2',
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
