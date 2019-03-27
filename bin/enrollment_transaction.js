const { pool } = require("./connection");
const moment = require("moment");
const { generateCode } = require("dhis2-uid");
const { convertCsvToJson } = require("./configManager");
const _ = require("lodash");
const { parallelLimit } = require("async");

let tei_entered = 0;
const enrollmentTranscation = async (
  teiValues,
  teiAttributeValues,
  programInstanceValues
) => {
  // note: we don't try/catch this because if connecting throws an exception
  // we don't need to dispose of the client (it will be undefined)
  const client = await pool.connect();
  const getNextIDQuery = `SELECT nextval('hibernate_sequence')`;
  const insertTeiQuery = `INSERT INTO trackedentityinstance(organisationunitid,trackedentitytypeid,lastupdatedby,uid,inactive,trackedentityinstanceid,created,lastupdated,deleted,createdatclient,lastupdatedatclient) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`;
  const insertTeiAttributesValuesQuery = `INSERT INTO trackedentityattributevalue(trackedentityattributeid,value,trackedentityinstanceid,created,lastupdated) VALUES ($1,$2,$3,$4,$5)`;
  const insertPrInstanceQuery = `INSERT INTO programinstance(programid,organisationunitid,incidentdate,enrollmentdate,status,uid,trackedentityinstanceid,programinstanceid,created,lastupdated,deleted,createdatclient,lastupdatedatclient) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`;

  try {
    await client.query("BEGIN");
    const teiIdRows = await client.query(getNextIDQuery);
    await client.query(insertTeiQuery, [
      ...teiValues,
      teiIdRows.rows[0].nextval,
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      false,
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      moment().format("YYYY-MM-DD HH:mm:ss.SSS")
    ]);

    for (const teiAttributeValue of teiAttributeValues) {
      const insertTeiAttributeValue = [
        ...teiAttributeValue,
        teiIdRows.rows[0].nextval,
        moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
        moment().format("YYYY-MM-DD HH:mm:ss.SSS")
      ];
      await client.query(
        insertTeiAttributesValuesQuery,
        insertTeiAttributeValue
      );
    }
    const programInsanceIdRows = await client.query(getNextIDQuery);
    await client.query(insertPrInstanceQuery, [
      ...programInstanceValues,
      teiIdRows.rows[0].nextval,
      programInsanceIdRows.rows[0].nextval,
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      false,
      moment().format("YYYY-MM-DD HH:mm:ss.SSS"),
      moment().format("YYYY-MM-DD HH:mm:ss.SSS")
    ]);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
};

const enrollTei = (
  teiValues,
  teiAttributeValues,
  programInstanceValues,
  callBackFn
) => {
  enrollmentTranscation(teiValues, teiAttributeValues, programInstanceValues)
    .then(value => {
      tei_entered++;
      console.log("The event Entered successfully: ", tei_entered);
      callBackFn(null, value);
    })
    .catch(e => {
      console.log("The event Errored");
      callBackFn(e, e.stack);
    });
};

const trackedEntityInstanceValueKeys = [
  "organisationunitid",
  "trackedentityid",
  "lastupdatedby",
  "uid"
];
const programInstanceValueKeys = [
  "programid",
  "organisationunitid",
  "incidentdate",
  "enrollmentdate",
  "status"
];

let noOfEventEntered = 0;
let noOfEventErrored = 0;
const enrollmentFunction = (csvTeiEnrollment, callBackFn) => {
  let programTei = {
    teiValues: [],
    teiAttributeValues: [],
    programInstanceValues: []
  };
  // organisationunitid,trackedentityid,lastupdateby, uid
  _.forEach(trackedEntityInstanceValueKeys, pkey => {
    programTei.teiValues.push(csvTeiEnrollment[pkey]);
  });
  //inactive
  programTei.teiValues.push(false);
  Object.keys(csvTeiEnrollment).forEach((key, indx) => {
    if (!csvTeiEnrollment[key] || csvTeiEnrollment[key] == "") {
      return;
    }
    if (
      trackedEntityInstanceValueKeys.indexOf(key) == -1 &&
      programInstanceValueKeys.indexOf(key) == -1
    ) {
      let teiAttributeEntry = [];
      //trackedentityattributeid,value
      teiAttributeEntry.push(...[key, csvTeiEnrollment[key]]);
      programTei.teiAttributeValues.push(teiAttributeEntry);
    }
  });
  //programid,organisationunitid,incidentdate,enrollmentdate
  _.forEach(programInstanceValueKeys, pkey => {
    programTei.programInstanceValues.push(csvTeiEnrollment[pkey]);
  });
  //uid
  programTei.programInstanceValues.push(...[generateCode()]);

  // Here pass the value to the event-transaction
  enrollTei(
    programTei.teiValues,
    programTei.teiAttributeValues,
    programTei.programInstanceValues,
    callBackFn
  );
};

const trackedEntityObject = trackedEntityEnrollments => {
  parallelLimit(
    trackedEntityEnrollments.map(
      csvTeiEnrollment =>
        function(callBackFn) {
          return enrollmentFunction(csvTeiEnrollment, callBackFn);
        }
    ),
    5,
    (err, results) => {
      console.info("=====Summary=======");
      console.info(
        "Number of trackedEntityEnrollments Successfully entered: ",
        results.length
      );
      console.info("Number of trackedEntityEnrollments errored: ", err);
      console.info("=========THE END========");
    }
  );
};

convertCsvToJson(trackedEntityObject);
