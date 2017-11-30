const { pool } = require('./connection');
const moment = require('moment');
const { convertCsvToJson } = require('./configManager');
const { mapSeries } = require('async');

let teiDeleted = 0;

const deleteTeiTransaction = async teID => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  const deleteTeiAttributeValue = `DELETE FROM trackedentityattributevalue where trackedentityinstanceid = ${
    teID
  }`;
  const deleteProgramInstance = `DELETE FROM programinstance where trackedentityinstanceid = ${
    teID
  }`;
  const deleteTei = `DELETE FROM trackedentityinstance where trackedentityinstanceid = ${
    teID
  }`;

  console.log(deleteTeiAttributeValue);

  try {
    await client.query('BEGIN');
    await client.query(deleteTeiAttributeValue);
    await client.query(deleteProgramInstance);
    await client.query(deleteTei);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
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
      console.log('The tei Errored');
      callBackFn(e, e.stack);
    });
};

const run = teIDs => {
  mapSeries(
    teIDs,
    (teID, callBackFn) => {
      deleteTei(teID['trackedentityinstanceid'], callBackFn);
    },
    (error, results) => {
      if (error) {
        console.log(error);
      }
      console.info('=====Summary=======');
      console.info('Number of event Successfully entered: ', results.length);
      console.info('Number of events errored: ', error);
      console.info('=========THE END========');
    }
  );
};

convertCsvToJson(run);
