const { pool } = require("./connection");
const { convertCsvToJson } = require("./configManager");
const { parallelLimit } = require("async");

let dataValueUpdated = 0;
const updateDataValueTransaction = async ({
  trackedentityinstanceid,
  trackedentityattributeid,
  value
}) => {
  const sqlStatement = `UPDATE trackedentityattributevalue SET value =${value} WHERE trackedentityattributeid=22863 and trackedentityinstanceid =${trackedentityinstanceid} and trackedentityattributeid =${trackedentityattributeid};`;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(sqlStatement);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

const updateDataValue = (dataValue, callBackFn) => {
  return updateDataValueTransaction(dataValue)
    .then(value => {
      dataValueUpdated++;
      console.log("The dataValues updated successfully: ", dataValueUpdated);
      callBackFn(null, value);
    })
    .catch(e => {
      console.log("The dataValues Errored");
      callBackFn(e, e.stack);
    });
};

const dataValueObject = async dataValues => {
  parallelLimit(
    dataValues.map(
      dataValue =>
        function(callBackFn) {
          return updateDataValue(dataValue, callBackFn);
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

convertCsvToJson(dataValueObject);

const chunkArray = (myArray, chunk_size) => {
  var results = [];

  while (myArray.length) {
    results.push(myArray.splice(0, chunk_size));
  }

  return results;
};
