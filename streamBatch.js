const stream = require('stream');
const http = require('http');
const SnowflakeConnector = require('./SnowflakeConnector');
const { promisify } = require('util');
const KafkaProducer = require('./KafkaProducer');
const MssqlUpserter = require('./MssqlUpserter');
const { log } = console;

const MAX_RECORDS = 10_000;
const start = async () => {
  const resourceMonitor = await import('monitor-resources-js');
  const monitorMemory = resourceMonitor.monitorMemory;
  monitorMemory();
  const sfInstance = await SnowflakeConnector.getInstance();
  const stmt = await sfInstance.getStmt();

  const rowsNumber = stmt.getNumRows();
  log('Number of rows:', rowsNumber);

  // stream 10 by 10
  let start = 0;
  let end = 9;

  const producer = await KafkaProducer.init({ topic: 'a.b.c' });
  const mssql = await MssqlUpserter.init({ table: '[dbo].[customer2]' });

  while (start < rowsNumber) {
    log('Streaming from ', start, 'to', end);
    const dataStream = stmt.streamRows({
      start,
      end,
    });
    let isPaused = false;
    let running = false;
    let count = 0;
    const read = new stream.Readable({
      objectMode: true,
      read(size) {
        if (isPaused) {
          log('resuming');
          dataStream.resume();
          isPaused = false;
        }
        if (running) {
          return;
        }

        running = true;

        dataStream.on('error', err => {
          log('read error');
          throw err;
        });

        dataStream?.on('finish', () => {
          log('read finish');
          this.push(null);
        });

        dataStream?.on('end', () => {
          log('read end');
          this.push(null);
        });

        let count = 0;
        dataStream?.on('data', row => {
          log('r', row.C_CUSTKEY);
          const state = this.push(row);

          if (++count === MAX_RECORDS) {
            this.push(null);
          }

          if (!state) {
            log('pausing');
            isPaused = true;
            dataStream.pause();
          }
        });
      },
    });

    const transform = new stream.Transform({
      objectMode: true,
      objectMode: true,
      async transform(chunk, encode, callback) {
        const resp = await mssql.merge(chunk);
        log('t', resp.rowsAffected);

        // await new Promise(resolve => setTimeout(resolve, 200));
        return callback(null, chunk);
      },
    });

    const writer = new stream.Writable({
      objectMode: true,
      async write(chunk, encode, callback) {
        log('w', chunk.C_CUSTKEY);
        const message = {
          key: `${new Date().toISOString()}_${chunk.C_CUSTKEY}`,
          value: JSON.stringify(chunk),
        };

        const resp = await producer.push(message);
        // await new Promise(resolve => setTimeout(resolve, 20));
        callback();
      },
    });

    const pipeline = promisify(stream.pipeline);
    await pipeline(read, transform, writer);

    start = end + 1;
    end = end + start;
    log('updated ranges', start, end);
  }
};

http.createServer().listen(3000, () => log('running'));
start()
  .then(() => console.log('Done'))
  .catch(console.error);
