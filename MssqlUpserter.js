const sql = require('mssql');

const timeout = 120_000_000;
const sqlConfig = {
  user: process.env.MSSQL_DB_USER,
  password: process.env.MSSQL_DB_PWD,
  database: process.env.MSSQL_DB_NAME,
  server: process.env.MSSQL_DB_SERVER,
  requestTimeout: timeout,
  options: {
    trustServerCertificate: true,
  },
  pool: {
    max: 1000,
    min: 1,
    idleTimeoutMillis: timeout,
    acquireTimeoutMillis: timeout,
    createTimeoutMillis: timeout,
    destroyTimeoutMillis: timeout,
    reapIntervalMillis: timeout,
    createRetryIntervalMillis: timeout,
  },
};

class MssqlUpserter {
  constructor(connection, table = '[dbo].[customer]') {
    this.connection = connection;
    this.table = table;
  }

  static async init({ table }) {
    if (!table) {
      throw new Error('Missing the table name when initiating ' + MssqlUpserter.name);
    }

    const pool = new sql.ConnectionPool(sqlConfig);
    await pool.connect();
    // await sql.connect(sqlConfig);

    return new MssqlUpserter(pool, table);
  }

  async select(data) {
    const query = `SELECT * FROM ${this.table} WHERE C_CUSTKEY = @C_CUSTKEY`;

    const request = new sql.Request();

    request.on('error', console.error);

    request.input('C_CUSTKEY', sql.Int, data.C_CUSTKEY);
    const results = await request.query(query);

    return results.recordset;
  }

  async insert(data) {
    const statement = new sql.PreparedStatement();
    statement.on('error', console.error);

    let { query, values } = Object.keys(data).reduce(
      (queryPart, columnName) => {
        let type = typeof data[columnName] === 'string' ? 'NVARCHAR' : 'Numeric';

        if (columnName === 'C_CUSTKEY') {
          type = 'Int';
        }

        if (columnName === 'createdAt') {
          type = 'DateTime';
        }

        statement.input(columnName, sql[type]);

        return { query: `${queryPart.query}${columnName},`, values: `${queryPart.values}@${columnName},` };
      },
      { query: `INSERT INTO ${this.table} (`, values: '(' }
    );

    query = query.slice(0, -1) + ')';
    values = values.slice(0, -1) + ');';

    const finalQuery = query + ' VALUES ' + values;

    await statement.prepare(finalQuery);
    const results = await statement.execute({ ...data, createdAt: new Date() });

    return results;
  }

  async delete(data) {
    const statement = new sql.PreparedStatement();
    statement.on('error', console.error);

    const query = `DELETE FROM ${this.table} WHERE C_CUSTKEY = @C_CUSTKEY`;

    statement.input('C_CUSTKEY', sql.Int);

    await statement.prepare(query);
    const results = await statement.execute({ C_CUSTKEY: data.C_CUSTKEY });

    return results;
  }

  async merge(data) {
    const statement = new sql.PreparedStatement(this.connection);
    statement.on('error', console.error);

    let { query, asSource, matched, notMatched, values } = Object.keys(data).reduce(
      (queryPart, columnName) => {
        let type = typeof data[columnName] === 'string' ? 'NVARCHAR' : 'Numeric';

        if (columnName === 'C_CUSTKEY') {
          type = 'Int';
        }

        if (columnName === 'createdAt') {
          type = 'DateTime';
        }

        statement.input(columnName, sql[type]);

        const query = `${queryPart.query}@${columnName},`;
        const asSource = `${queryPart.asSource}${columnName},`;
        const matched = `${queryPart.matched}Target.${columnName} = Source.${columnName},`;
        const notMatched = `${queryPart.notMatched}${columnName},`;
        const values = `${queryPart.values}Source.${columnName},`;
        return { query, asSource, matched, notMatched, values };
      },
      {
        query: `MERGE INTO ${this.table} WITH (ROWLOCK) AS Target\nUSING ( VALUES (`,
        asSource: ' AS Source (',
        matched: ' WHEN MATCHED THEN UPDATE SET ',
        notMatched: ' WHEN NOT MATCHED BY TARGET THEN INSERT (',
        values: ' VALUES (',
      }
    );

    query = query.slice(0, -1) + ')\n)';
    asSource = asSource.slice(0, -1) + ') ON Target.C_CUSTKEY = Source.C_CUSTKEY ';
    matched = matched.slice(0, -1);
    notMatched = notMatched.slice(0, -1) + ')';
    values = values.slice(0, -1) + ');';

    const finalQuery = `${query}\n${asSource}\n${matched}\n${notMatched}\n${values}`;

    await statement.prepare(finalQuery);
    const results = await statement.execute(data);

    return results;
  }
}

module.exports = MssqlUpserter;
