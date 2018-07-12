const fs = require("fs");
const argv = require("yargs").argv;
const csvtojson = require("csvtojson");

const convertCsvToJson = doneFn => {
  const Converter = csvtojson.Converter;
  const converter = new Converter({});
  converter.on("end_parsed", doneFn);
  fs.createReadStream(getArgs()["file"]).pipe(converter);
};

const getArgs = () => {
  return argv;
};

module.exports = { convertCsvToJson, getArgs };
