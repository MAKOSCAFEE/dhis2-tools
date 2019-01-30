const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { parallelLimit } = require("async");

let teiUpdated = 0;
const updateTeiLocations = async (teID, organisationunitid) => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await updateTeiLocationQueries(client, teID, organisationunitid);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const updateTeiLocationQueries = async (client, teID, organisationunitid) => {
  const updateTeiTable = `UPDATE trackedentityinstance SET organisationunitid = ${organisationunitid} where trackedentityinstanceid = ${teID}`;
  const updateProgramInstanceTable = `UPDATE programinstance SET  organisationunitid = ${organisationunitid}  where trackedentityinstanceid = ${teID}`;
  return client.query(updateTeiTable), client.query(updateProgramInstanceTable);
};

const updateLocations = (teID, organisationunitid, callBackFn) => {
  updateTeiLocations(teID, organisationunitid)
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
      ({ trackedentityinstanceid, organisationunitid }) =>
        function(callBackFn) {
          return updateLocations(
            trackedentityinstanceid,
            organisationunitid,
            callBackFn
          );
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
