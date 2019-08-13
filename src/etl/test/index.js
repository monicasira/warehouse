/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.helloWorld = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  main();
  res.status(200).send(message);
  
};

function main() {
  // [START bigquery_add_column_load_append]
  // Import the Google Cloud client libraries
  const {BigQuery} = require('@google-cloud/bigquery');
  const {Storage} = require('@google-cloud/storage');
  const bucketName = 'srichand-ecomm-test';
  const filename = 'checkout_products4.csv'	;
  //blissful-flame-245410.finding_nemo.checkout_products
  // Instantiate client
  const bigquery = new BigQuery();
  const storageClient = new Storage();
  const datasetId = 'ecomm_test';
  const tableId = 'checkout_products4';
  async function addColumnLoadAppend() {
    //** create new table
	const [tableNew] = await bigquery
      .dataset(datasetId)
      .createTable(tableId);
    console.log(`New temp table created.`);

    // Retrieve destination table reference
    const [table] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .get();
    const destinationTableRef = table.metadata.tableReference;
    
    // Set load job options
    const options = {
      //schema: schema,
      schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
      skipLeadingRows: 1,
      autodetect: true,
      writeDisposition: 'WRITE_APPEND',
      createDisposition: 'CREATE_IF_NEEDED',
      destinationTable: destinationTableRef,
    };
	
    // Load data from a google cloud storage into the table
    const [job] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .load(storageClient.bucket(bucketName).file(filename), options);
      //.load(fileName, options);

    console.log(`Job ${job.id} load csv to table completed.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }

    //** Delete matching line_items from original table
    const query = `DELETE \`data-warehouse-srichand.ecomm_test.checkout_products2\` i
		WHERE i.id IN (SELECT id from \`data-warehouse-srichand.ecomm_test.checkout_products4\`)`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options2 = {
      query: query,
      // Location must match that of the dataset(s) referenced in the query.
      location: 'asia-southeast1',
    };

    // Run the query as a job
    const [job2] = await bigquery.createQueryJob(options2);
    console.log(`Job ${job2.id} started.`);

    // Wait for the query to finish
    const [rows] = await job2.getQueryResults();
    console.log(`Job ${job2.id} deleted matching from original completed.`);

	//** append new table to original table
    const query3 = `Select * from \`data-warehouse-srichand.ecomm_test.checkout_products4\``;

    const tableId3 = 'checkout_products2';
    const [table3] = await bigquery
      .dataset(datasetId)
      .table(tableId3)
      .get();
    const destinationTableRef3 = table3.metadata.tableReference;

    // Set load job options
    const options3 = {
      query: query3,
      schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
      skipLeadingRows: 1,
      writeDisposition: 'WRITE_APPEND',
      destinationTable: destinationTableRef3,
    };
	
	const [job3] = await bigquery.createQueryJob(options3);
    console.log(`Job ${job3.id} started.`);

    // Wait for the query to finish
    const [rows3] = await job3.getQueryResults();
    console.log(`Job ${job3.id} adding rows to original completed.`);
	
    //** Delete table
	await bigquery
      .dataset(datasetId)
      .table(tableId)
      .delete();

    console.log(`Table ${tableId} deleted.`);

  }
  // [END bigquery_add_column_load_append]
  addColumnLoadAppend();
}
