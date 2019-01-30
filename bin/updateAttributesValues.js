const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { parallelLimit } = require("async");

let teiUpdated = 0;
const updateAttributesTables = async (trackedentityinstanceid, attVArray) => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const attObject of attVArray) {
      const attributeID = Object.keys(attObject)[0];
      const attributeValue = Object.values(attObject)[0];
      const updateTableQuery = `UPDATE trackedentityattributevalue SET value = '${attributeValue}' WHERE trackedentityinstanceid=${trackedentityinstanceid} AND trackedentityattributeid=${attributeID}`;
      await client.query(updateTableQuery);
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const updateAttributesValues = (csvData, callBackFn) => {
  const trackedentityinstanceid = csvData["trackedentityinstanceid"];
  const attV = Object.keys(csvData)
    .filter(key => key !== "trackedentityinstanceid")
    .map(key => ({ [key]: csvData[key] }));
  updateAttributesTables(trackedentityinstanceid, attV)
    .then(value => {
      teiUpdated++;
      console.log(
        "The TrackedEntityIsntance updated location successfully: ",
        teiUpdated
      );
      callBackFn(null, value);
    })
    .catch(e => {
      console.log("The tei Errored");
      callBackFn(e, e.stack);
    });
};

const run = teIDs => {
  parallelLimit(
    teIDs.map(
      csvData =>
        function(callBackFn) {
          return updateAttributesValues(csvData, callBackFn);
        }
    ),
    200,
    (error, results) => {
      if (error) {
        console.log(error);
      }
      console.info("=====Summary=======");
      console.info(
        "Number of TrackedEntity updated locations Successfully: ",
        results.length
      );
      console.info("Number of TrackedEntity update errored: ", error);
      console.info("=========THE END========");
    }
  );
};

convertCsvToJson(run);
