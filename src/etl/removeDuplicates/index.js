/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */

exports.ecommRemoveDuplicate = (event, context) => {
  const pubsubMessage = event.data;
  console.log(Buffer.from(pubsubMessage, 'base64').toString());
  return main();
};

async function main() {
  // [START bigquery_add_column_load_append]
  // Import the Google Cloud client libraries
  const {BigQuery} = require('@google-cloud/bigquery');
  // Instantiate client
  const bigqueryN = new BigQuery();
  const datasetBq = 'ecomm_production';
  const tableBq = 'transactions';
    
  // Retrieve destination table reference
  const [table] = await bigqueryN
    .dataset(datasetBq)
    .table(tableBq)
    .get();

  const destination = table.metadata.tableReference;


  await removeDuplicateFromTable(bigqueryN, datasetBq, tableBq, destination);
}


async function removeDuplicateFromTable(bigquery, datasetId, tableId, destinationTableRef){
  //** Delete matching line_items from original table
  const query = `SELECT *
                  FROM (
                    SELECT
                        *,
                        ROW_NUMBER()
                            OVER (PARTITION BY line_item_id)
                            as rn
                    FROM \`data-warehouse-srichand.${datasetId}.${tableId}\`
                  ) as no_dup
                  WHERE no_dup.rn = 1`;

  // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
  const options = {
    query: query,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'US',
    writeDisposition: 'WRITE_TRUNCATE',
    destinationTable: destinationTableRef,
  };
  
  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} start remove duplicate.`);

  await job.getQueryResults();

  const query2 = `SELECT * except (rn) FROM \`data-warehouse-srichand.${datasetId}.${tableId}\``;

  const options2 = {
    query: query2,
    location: 'US',
    writeDisposition: 'WRITE_TRUNCATE',
    destinationTable: destinationTableRef,
  };

  // Run the query as a job
  const [job2] = await bigquery.createQueryJob(options2);
  console.log(`Job ${job2.id} start to delete column row_number(rn)`);

  return job2
}

async function removeRowNumber(bigquery, datasetId, tableId, destinationTableRef){
  // remove row number columm
  const query2 = `SELECT * except (rn) FROM \`data-warehouse-srichand.${datasetId}.${tableId}\``;

  const options2 = {
    query: query2,
    location: 'US',
    writeDisposition: 'WRITE_TRUNCATE',
    destinationTable: destinationTableRef,
  };

  // Run the query as a job
  const [job2] = await bigquery.createQueryJob(options2);
  console.log(`Job ${job2.id} start to delete column row_number(rn)`);

  return job2
}
