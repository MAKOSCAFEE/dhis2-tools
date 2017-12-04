const {
  pool
} = require('./connection');
const moment = require('moment');
const {
  generateCodes,
  generateCode
} = require('dhis2-uid');
const {
  convertCsvToJson
} = require('./configManager');
const _ = require('lodash');
const {
  mapSeries
} = require('async');

let event_entered = 0;
const relationshipTranscation = async relationshipValues => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  const getRelationshipIDQuery = `SELECT nextval('hibernate_sequence')`;
  const insertRelationshipQuery = `INSERT INTO relationship(trackedentityinstanceaid,trackedentityinstancebid,relationshiptypeid,relationshipid) VALUES ($1,$2,$3,$4)`;

  try {
    await client.query('BEGIN');
    const idRows = await client.query(getRelationshipIDQuery);
    await client.query(insertRelationshipQuery, [
      ...relationshipValues,
      idRows.rows[0].nextval
    ]);

    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
};

const postRelationship = (relationshipValues, callBackFn) => {
  relationshipTranscation(relationshipValues)
    .then(value => {
      event_entered++;
      console.log('The event Entered successfully: ', event_entered);
      callBackFn(null, value);
    })
    .catch(e => {
      console.log('The event Errored');
      callBackFn(e, e.stack);
    });
};
const relatioshipValueKeys = [
  'trackedEntityInstanceA',
  'trackedEntityInstanceB',
  'relationshiptypeID'
]
const relationshipObject = relationships => {
  mapSeries(
    relationships,
    (relationship, callBackFn) => {
      let relationshipValues = [];
      _.forEach(relatioshipValueKeys, rkey => {
        relationshipValues.push(relationship[rkey]);
      });

      postRelationship(relationshipValues, callBackFn);
    },
    (err, results) => {
      console.info('=====Summary=======');
      console.info('Number of event Successfully entered: ', results.length);
      console.info('Number of events errored: ', err);
      console.info('=========THE END========');
    }
  );
};

convertCsvToJson(relationshipObject);