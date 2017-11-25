const { postData } = require('./bin/event-transaction');
const { generateCodes, generateCode } = require('dhis2-uid');
const { convertCsvToJson } = require('./bin/configManager');
const _ = require('lodash');
const { waterfall, mapSeries } = require('async');

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
