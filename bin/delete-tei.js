const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { mapSeries } = require("async");

let teiDeleted = 0;
const deleteTeiTransaction = async teID => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  const deleteTeiAttributeValue = `DELETE FROM trackedentityattributevalue where trackedentityinstanceid = ${teID}`;
  const getProgramStageInstances = prinstanceid =>
    `SELECT programstageinstanceid FROM programstageinstance WHERE programinstanceid =${prinstanceid}`;
  const deleteProgramStageInstance = prinstanceid =>
    `DELETE FROM programstageinstance WHERE programinstanceid =${prinstanceid}`;
  const deleteTrackedEntityDataValue = prStageId =>
    `DELETE FROM trackedentitydatavalue teidv WHERE teidv.programstageinstanceid = ${prStageId}`;
  const getProgramInstance = `SELECT programinstanceid FROM programinstance where trackedentityinstanceid = ${teID}`;
  const deleteProgramInstance = `DELETE FROM programinstance where trackedentityinstanceid = ${teID}`;
  const deleteRelationship = `DELETE FROM relationship WHERE trackedentityinstanceaid= ${teID} OR trackedentityinstancebid =${teID}`;
  const deleteTei = `DELETE FROM trackedentityinstance where trackedentityinstanceid = ${teID}`;

  try {
    await client.query("BEGIN");
    const { rows: programinstances } = await client.query(getProgramInstance);
    for (const prinstance of programinstances) {
      const prinstanceid = prinstance.programinstanceid;
      const { rows: programStageInstances } = await client.query(
        getProgramStageInstances(prinstanceid)
      );
      for (const row of programStageInstances) {
        const prStageId = row.programstageinstanceid;
        await client.query(deleteTrackedEntityDataValue(prStageId));
      }
      await client.query(deleteProgramStageInstance(prinstanceid));
    }
    await client.query(deleteTeiAttributeValue);
    await client.query(deleteProgramInstance);
    await client.query(deleteRelationship);
    await client.query(deleteTei);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const deleteTei = (teID, callBackFn) => {
  deleteTeiTransaction(teID)
    .then(value => {
      teiDeleted++;
      callBackFn(null, value);
    })
    .catch(e => {
      console.log("The tei Errored");
      callBackFn(e, e.stack);
    });
};

const run = teIDs => {
  mapSeries(
    teIDs,
    (teID, callBackFn) => {
      deleteTei(teID["trackedentityinstanceid"], callBackFn);
    },
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
