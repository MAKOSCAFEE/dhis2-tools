const { Pool, Client } = require('pg');
const credentials = require('./env.json');

const pool = new Pool(credentials);

const client = new Client(credentials);

client.connect();

module.exports = { pool, client };
