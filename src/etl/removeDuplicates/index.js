/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */

exports.ecommRemoveDuplicate = (event, context) => {
  const pubsubMessage = event.data;
  console.log(Buffer.from(pubsubMessage, 'base64').toString());
  main();
};

async function main() {
  // [START bigquery_add_column_load_append]
  // Import the Google Cloud client libraries
  const {BigQuery} = require('@google-cloud/bigquery');
  // Instantiate client
  const bigquery = new BigQuery();
  const datasetId = 'ecomm_production';
  const tableId = 'transactions_backup';
  async function removeDuplicates() {

    // Retrieve destination table reference
    const [table] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .get();
    const destinationTableRef = table.metadata.tableReference;
   
    async function removeDuplicateFromTable(){
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

      const [rows] = await job.getQueryResults();
      console.log(`Job ${job.id} complete delete duplicate from transactions table.`);

      return rows
    }
    

    async function removeRowNumber(){
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

    let promisesArr = []
    let promise1 = await removeDuplicateFromTable();
    let promise2 = await removeRowNumber();
    promisesArr.push(promise1)
    promisesArr.push(promise2)
    results = await Promise.all(promisesArr)
    return results
  }
  // [END bigquery_add_column_load_append]
  removeDuplicates();
}
