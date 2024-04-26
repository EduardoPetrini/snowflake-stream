const dotenv = require('dotenv');
const snowflake = require('snowflake-sdk');

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

class SnowflakeConnector {
  constructor(sqlText, connection) {
    this.sqlText = sqlText;
    this.connection = connection;
  }

  static async getInstance() {
    log('creating connection');
    const connection = snowflake.createConnection(credentials);
    log('connecting');
    await new Promise((resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn))));

    const instanced = new SnowflakeConnector(sqlText, connection);
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

module.exports = SnowflakeConnector;
