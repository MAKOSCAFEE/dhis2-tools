const { pool } = require('./connection');
const moment = require('moment');
const { generateCodes, generateCode } = require('dhis2-uid');
const { convertCsvToJson } = require('./configManager');
const _ = require('lodash');
const { mapSeries } = require('async');

let event_entered = 0;
const eventTranscation = async (psiValues, tedValues) => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  const getEventIDQuery = `SELECT nextval('hibernate_sequence')`;
  const insertPsiQuery = `INSERT INTO programstageinstance(programinstanceid,programstageid,attributeoptioncomboid,storedby,completedby,organisationunitid,status,completeddate,executiondate,uid,programstageinstanceid,created,lastupdated,deleted,duedate,createdatclient,lastupdatedatclient) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING programstageinstanceid`;
  const insertTedQuery = `INSERT INTO trackedentitydatavalue(dataelementid,value,providedelsewhere,storedby,programstageinstanceid,created,lastupdated) VALUES ($1,$2,$3,$4,$5,$6,$7)`;

  try {
    await client.query('BEGIN');
    const idRows = await client.query(getEventIDQuery);
    const { rows } = await client.query(insertPsiQuery, [
      ...psiValues,
      idRows.rows[0].nextval,
      moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
      moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
      false,
      moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
      moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
      moment().format('YYYY-MM-DD HH:mm:ss.SSS')
    ]);

    for (const tedValue of tedValues) {
      const insertTedValue = [
        ...tedValue,
        rows[0].programstageinstanceid,
        moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
        moment().format('YYYY-MM-DD HH:mm:ss.SSS')
      ];
      await client.query(insertTedQuery, insertTedValue);
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
};

const postData = (psiValues, tedValues, callBackFn) => {
  eventTranscation(psiValues, tedValues)
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

const programstageinstanceValueKeys = [
  'programinstanceid',
  'programstageid',
  'attributeoptioncomboid',
  'storedby',
  'completedby',
  'organisationunitid',
  'status',
  'completeddate',
  'executiondate'
];

let noOfEventEntered = 0;
let noOfEventErrored = 0;
const eventObject = events => {
  mapSeries(
    events,
    (csvEvent, callBackFn) => {
      let pEvent = {
        pstageValues: [],
        teiValues: []
      };
      _.forEach(programstageinstanceValueKeys, pkey => {
        pEvent.pstageValues.push(csvEvent[pkey]);
      });
      pEvent.pstageValues.push(...[generateCode()]);
      Object.keys(csvEvent).forEach((key, indx) => {
        if (!csvEvent[key] || csvEvent[key] == '') {
          return;
        }
        if (programstageinstanceValueKeys.indexOf(key) == -1) {
          let teiEntry = [];
          teiEntry.push(...[key, csvEvent[key], false, csvEvent['storedby']]);
          pEvent.teiValues.push(teiEntry);
        }
      });

      // Here pass the value to the event-transaction
      const eventEntered = postData(
        pEvent.pstageValues,
        pEvent.teiValues,
        callBackFn
      );
    },
    (err, results) => {
      console.info('=====Summary=======');
      console.info('Number of event Successfully entered: ', results.length);
      console.info('Number of events errored: ', err);
      console.info('=========THE END========');
    }
  );
};

convertCsvToJson(eventObject);
