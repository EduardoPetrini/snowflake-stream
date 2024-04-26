const http = require('http');
const snowflake = require('snowflake-sdk');
const EventEmitter = require('events');
const dotenv = require('dotenv');
const { log } = console;

dotenv.config();

const credentials = {
  account: process.env.SF_ACCOUNT,
  database: process.env.SF_DATABASE,
  warehouse: process.env.SF_WAREHOUSE,
  username: process.env.SF_USERNAME,
  password: process.env.SF_PASSWORD,
};

const sqlText = process.env.SQL_TEXT;

const start = async () => {
  log('creating connection');
  const connection = snowflake.createConnection(credentials);
  log('connecting');
  await new Promise((resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn))));

  log('getting statement');
  const statement = await new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      streamResult: true,
      complete: (err, stmt) => (err ? reject(err) : resolve(stmt)),
    });
  });

  log('getting stream');
  const dataStream = statement?.streamRows();

  const event = new EventEmitter();

  event.on('pause', () => {
    log('pausing');
    dataStream.pause();
    log('waiting to resume')
    setTimeout(() => event.emit('resume'), 1000);
  });

  event.on('resume', () => {
    log('resuming');
    dataStream.resume();
  });

  let count = 0;
  dataStream.on('data', row => {
    log('r', row.C_CUSTKEY);
    if (++count % 10 === 0) {
      event.emit('pause');
    }
  });

  const server = http.createServer();
  server.listen(3003, () => {
    console.log('hm');
  });
};

start()
  .then(() => console.log('Done'))
  .catch(console.error);
