const stream = require('stream');
const SnowflakeConnector = require('./SnowflakeConnector');
const KafkaProducer = require('./KafkaProducer');
const MssqlUpserter = require('./MssqlUpserter');
const { log } = console;

const MAX_RECORDS = 10_000;
const start = async () => {
  const resourceMonitor = await import('monitor-resources-js');
  const monitorMemory = resourceMonitor.monitorMemory;
  monitorMemory();
  const sfInstance = await SnowflakeConnector.getInstance();
  const dataStream = await sfInstance.getStreamData();

  const producer = await KafkaProducer.init({ topic: 'd.e.f' });
  const mssql = await MssqlUpserter.init({ table: '[dbo].[customer1]' });

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
        throw err;
      });

      dataStream?.on('finish', () => {
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
    async transform(chunk, encode, callback) {
      const resp = await mssql.merge(chunk);
      log('t', resp.rowsAffected);
      // await new Promise(resolve => setTimeout(resolve, 22));
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
      // await new Promise(resolve => setTimeout(resolve, 200));
      callback();
    },
  });

  return new Promise((resolve, reject) => {
    stream.pipeline(read, transform, writer, err => {
      if (err) return reject(err);
      resolve(true);
    });
  });
};

start()
  .then(() => console.log('Done'))
  .catch(console.error);
