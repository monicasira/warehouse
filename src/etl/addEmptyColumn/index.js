
exports.ecommAddEmptyColumn = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  main();
  res.status(200).send(message);
  
};

function main() {

    const {BigQuery} = require('@google-cloud/bigquery');
    const {Storage} = require('@google-cloud/storage');
    const bigquery = new BigQuery();
    const fs = require('fs');

    async function addEmptyColumn() {
      // Adds an empty column to the schema.

      /**
       * TODO(developer): Uncomment the following lines before running the sample.
       */
      const datasetId = 'ecomm_test';
      const tableId = 'transactions';
      const columns = [{name: 'size2', type: 'STRING'},{name: 'size3', type: 'STRING'}];

      //storage
      const bucketName = 'srichand-ecomm-test';
      const filename = 'schema-files/schema1.json';
      const storage = new Storage();
      const bucket = storage.bucket(bucketName);
      const remoteFile = bucket.file(filename);
      var obj;
      fs.readFile(remoteFile, 'utf8', function (err, data) {
        if (err) throw err;
        obj = JSON.parse(data);
      });
      console.log('json object', obj)

      // Retrieve current table metadata
      const table = bigquery.dataset(datasetId).table(tableId);
      const [metadata] = await table.getMetadata();

      // Update table schema
      const schema = metadata.schema;
      const new_schema = schema;
      for(let i = 0; i < columns.length; i++){
        new_schema.fields.push(columns[i]);
      }
      metadata.schema = new_schema;

      //commenting out because want to test reading data

      //const [result] = await table.setMetadata(metadata);
      //console.log(result.schema.fields);
    }
    // [END bigquery_add_empty_column]
    addEmptyColumn();
}

async function listFilesByPrefix() {
  // [START storage_list_files_with_prefix]
  // Imports the Google Cloud client library
  

  // Creates a client
  const storage = new Storage();

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  const bucketName = 'srichand-ecomm-test';
  const prefix = 'schema-files/';
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
