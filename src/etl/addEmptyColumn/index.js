
exports.ecommAddEmptyColumn = (req, res) => {
  let message = req.query.message || req.body.message || 'Hello World!';
  main();
  res.status(200).send(message);
  
};

function main() {

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function addEmptyColumn() {
      // Adds an empty column to the schema.

      /**
       * TODO(developer): Uncomment the following lines before running the sample.
       */
      const datasetId = 'ecomm_test';
      const tableId = 'transactions';
      const column = {name: 'size', type: 'STRING'};

      // Retrieve current table metadata
      const table = bigquery.dataset(datasetId).table(tableId);
      const [metadata] = await table.getMetadata();

      // Update table schema
      const schema = metadata.schema;
      const new_schema = schema;
      new_schema.fields.push(column);
      metadata.schema = new_schema;

      const [result] = await table.setMetadata(metadata);
      console.log(result.schema.fields);
    }
    // [END bigquery_add_empty_column]
    addEmptyColumn();
}
