const snowflake = require('snowflake-sdk');
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

let isPaused = false;
let isDone = false;
function nextItem(dataStream) {
  log('Reading next...');
  let done = false;

  const end = new Promise(resolve => {
    dataStream.once('finish', resolve);
  }).then(() => {
    done = true;
  });

  const iterator = {
    [Symbol.iterator]() {
      return this;
    },
    async next() {
      const promise = new Promise(resolve => {
        dataStream.once('data', value => {
          resolve(value);
          dataStream.pause();
        });
        dataStream.resume();
      });

      return {
        value: await Promise.race([promise, end]),
        done,
      };
    },
  };

  return iterator;
}

class SnowflakeGenerator {
  constructor(sqlText, connection) {
    this.sqlText = sqlText;
    this.connection = connection;
  }

  static async getInstance(sqlText, credentials) {
    log('creating connection');
    const connection = snowflake.createConnection(credentials);
    log('connecting');
    await new Promise((resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn))));

    const instanced = new SnowflakeGenerator(sqlText, connection);
    log('Getting the stream data');
    await instanced.getStreamData();

    return instanced;
  }

  async getStmt() {
    log('getting statement');
    if (this.statement) {
      return this.statement;
    }
    const statement = await new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText,
        streamResult: true,
        complete: (err, stmt) => (err ? reject(err) : resolve(stmt)),
      });
    });

    this.statement = statement;
    return statement;
  }

  async getStreamData() {
    log('getting streamData');
    if (this.dataStream) {
      return this.dataStream;
    }

    if (!this.statement) {
      await this.getStmt();
    }
    const dataStream = this.statement.streamRows();
    this.dataStream = dataStream;
    return dataStream;
  }

  async getNextItem() {
    const result = await nextItem(this.dataStream);
    const ni = await result.next();
    return result;
  }
}

const start = async () => {
  log('Getting the instance...');
  const sfGen = await SnowflakeGenerator.getInstance(sqlText, credentials);
  const dataStream = await sfGen.getStreamData();
  const iterator = nextItem(dataStream);

  log('Getting result1...');
  const { value: result1 } = await iterator.next();
  log('Getting result2...');
  const { value: result2 } = await iterator.next();
  log('Getting result3...');
  const { value: result3 } = await iterator.next();

  console.log(result1.C_CUSTKEY);
  console.log(result2.C_CUSTKEY);
  console.log(result3.C_CUSTKEY);

};

// start()
//   .then(() => console.log('Done'))
//   .catch(console.error);

module.exports = { SnowflakeGenerator };
