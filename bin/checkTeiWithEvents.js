const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { parallelLimit } = require("async");
const fs = require("fs");

let teiDeleted = 0;
let BeneficiariesWithData = [];
const checkTeiWithEvents = async teID => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await getTeiRelatedQueries(client, teID);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const getTeiRelatedQueries = async (client, teID) => {
  const getProgramInstance = `SELECT programinstanceid FROM programinstance where trackedentityinstanceid = ${teID}`;
  const getProgramStageInstances = prinstanceid =>
    `SELECT programstageinstanceid FROM programstageinstance WHERE programinstanceid =${prinstanceid}`;
  const { rows: programinstances } = await client.query(getProgramInstance);

  for (const prinstance of programinstances) {
    const prinstanceid = prinstance.programinstanceid;
    const { rows: programStageInstances } = await client.query(
      getProgramStageInstances(prinstanceid)
    );

    if (programStageInstances.length) {
      BeneficiariesWithData.push(teID);
    }
  }
};

const deleteTei = (teID, callBackFn) => {
  checkTeiWithEvents(teID)
    .then(value => {
      teiDeleted++;
      console.log(
        "The TrackedEntityIsntance successfully Checked: ",
        teiDeleted
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
      ({ trackedentityinstanceid }) =>
        function(callBackFn) {
          return deleteTei(trackedentityinstanceid, callBackFn);
        }
    ),
    200,
    (error, results) => {
      if (error) {
        console.log(error);
      }
      writeToJsonFile();
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

const writeToJsonFile = () => {
  fs.writeFile("temp.json", JSON.stringify(BeneficiariesWithData), function(
    err
  ) {
    if (err) throw err;
    console.log("complete");
  });
};

convertCsvToJson(run);
