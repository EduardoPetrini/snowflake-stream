const dotenv = require('dotenv');
const { SnowflakeGenerator } = require('./iterator');
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

function* nextItemOld(dataStream) {
  log('Reading next...');
  let done = false;

  const end = new Promise(resolve => {
    dataStream.once('finish', resolve);
  }).then(() => {
    done = true;
  });

  async function* iterator() {
    while (!done) {
      const value = new Promise(resolve => {
        dataStream.once('data', value => {
          resolve(value);
          dataStream.pause();
        });
        dataStream.resume();
      });

      yield value;
    }
  }

  return iterator();
}

async function* nextItem(dataStream) {
  log('Reading next...');
  let done = false;

  const finish = new Promise(resolve =>
    dataStream.once('finish', () => {
      resolve(null);
      done = true;
    })
  );

  while (!done) {
    const promise = new Promise(resolve => {
      dataStream.once('data', row => {
        resolve(row);
        dataStream.pause();
      });
      dataStream.resume();
    });

    yield await Promise.race([promise, finish]);
  }
}

async function test() {
  const genIns = await SnowflakeGenerator.getInstance(sqlText, credentials);
  const streamData = await genIns.getStreamData();
  const iterator = nextItem(streamData);
  try {
    const { value: nextRow1 } = await iterator.next();
    console.log(nextRow1.C_CUSTKEY); // Output: 'row 1'

    const { value: nextRow2 } = await iterator.next();
    console.log(nextRow2.C_CUSTKEY); // Output: 'row 2'

    const { value: nextRow3 } = await iterator.next();
    console.log(nextRow3.C_CUSTKEY); // Output: 'row 3'

    const { value: nextRow4 } = await iterator.next();
    console.log(nextRow4.C_CUSTKEY); // Output: null

    await new Promise(resolve => setTimeout(resolve, 10000));

    const { value: nextRow5 } = await iterator.next();
    console.log(nextRow5.C_CUSTKEY); // Output: null

    await new Promise(resolve => setTimeout(resolve, 5000));
  } catch (error) {
    console.error('Error:', error);
  }
}

test();
