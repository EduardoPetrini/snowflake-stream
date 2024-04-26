const stream = require('stream');
const SnowflakeConnector = require('./SnowflakeConnector');
const { log } = console;

const start = async () => {
  const sfInstance = await SnowflakeConnector.getInstance();
  const dataStream = await sfInstance.getStreamData();

  let isPaused = false;
  let running = false;
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
    transform(chunk, encode, callback) {
      console.log(chunk.C_CUSTKEY, 't');

      callback(null, chunk);
    },
  });

  const writer = new stream.Writable({
    objectMode: true,
    async write(chunk, encode, callback) {
      setTimeout(() => {
        console.log('w', chunk.C_CUSTKEY);
        // log('write');
        callback();
      }, 1000);
    },
  });

  // const pipeline = promisify(stream.pipeline);
  // await pipeline(read, writer);
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
