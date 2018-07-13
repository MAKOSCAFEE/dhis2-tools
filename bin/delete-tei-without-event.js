const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { parallelLimit } = require("async");

let teiDeleted = 0;
const deleteTeiTransaction = async teID => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await deleteTeiRelatedQueries(client, teID);
    await client.query(deleteTei);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const deleteTeiRelatedQueries = async (client, teID) => {
  const deleteTeiAttributeValue = `DELETE FROM trackedentityattributevalue where trackedentityinstanceid = ${teID}`;
  const deleteProgramInstance = `DELETE FROM programinstance where trackedentityinstanceid = ${teID}`;
  const deleteRelationship = `DELETE FROM relationship WHERE trackedentityinstanceaid= ${teID} OR trackedentityinstancebid =${teID}`;
  const deletetrackedentityattributevalueaudit = `DELETE FROM trackedentityattributevalueaudit WHERE trackedentityinstanceid = ${teID}`;
  return (
    client.query(deleteTeiAttributeValue),
    client.query(deleteProgramInstance),
    client.query(deleteRelationship),
    client.query(deletetrackedentityattributevalueaudit)
  );
};

const deleteTei = (teID, callBackFn) => {
  deleteTeiTransaction(teID)
    .then(value => {
      teiDeleted++;
      console.log("The TrackedEntityIsntance successfully: ", teiDeleted);
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
      ({ trackedentityinstanceid }) =>
        function(callBackFn) {
          return deleteTei(trackedentityinstanceid, callBackFn);
        }
    ),
    50,
    (error, results) => {
      if (error) {
        console.log(error);
      }
      console.info("=====Summary=======");
      console.info(
        "Number of TrackedEntity deleted Successfully: ",
        results.length
      );
      console.info("Number of TrackedEntity errored: ", error);
      console.info("=========THE END========");
    }
  );
};

convertCsvToJson(run);