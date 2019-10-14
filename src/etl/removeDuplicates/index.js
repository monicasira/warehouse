/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.ecommRemoveDuplicates = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  main();
  res.status(200).send(message);
  
};

function main() {
  // [START bigquery_add_column_load_append]
  // Import the Google Cloud client libraries
  const {BigQuery} = require('@google-cloud/bigquery');
  // Instantiate client
  const bigquery = new BigQuery();
  const datasetId = 'ecomm_test';
  const tableId = 'transactions';
  async function removeDuplicates() {

    // Retrieve destination table reference
    const [table] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .get();
    const destinationTableRef = table.metadata.tableReference;
    
    //** Delete matching line_items from original table
    const query = `SELECT *
                    FROM (
                      SELECT
                          *,
                          ROW_NUMBER()
                              OVER (PARTITION BY line_item_id)
                              row_number
                      FROM \`data-warehouse-srichand.${datasetId}.${tableId}\`
                    )
                    WHERE row_number = 1`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
      query: query,
      // Location must match that of the dataset(s) referenced in the query.
      location: 'asia-east2',
      writeDisposition: 'WRITE_TRUNCATE',
      destinationTable: destinationTableRef,
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();
    console.log(`Job ${job.id} deleted duplicates with row 1.`);

    const query2 = `SELECT * except(row_number) FROM \`data-warehouse-srichand.${datasetId}.${tableId}\``;

    const options2 = {
      query: query2,
      location: 'asia-east2',
      writeDisposition: 'WRITE_TRUNCATE',
      destinationTable: destinationTableRef,
    };

    // Run the query as a job
    const [job2] = await bigquery.createQueryJob(options);
    console.log(`Job remove row number ${job2.id} started.`);

  }
  // [END bigquery_add_column_load_append]
  removeDuplicates();
}
