const SnowflakeConnector = require('./SnowflakeConnector');
const { log } = console;

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

const start = async () => {
  log('Getting the instance...');
  const sfInstance = await SnowflakeConnector.getInstance();
  const dataStream = await sfInstance.getStreamData();
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

start()
  .then(() => console.log('Done'))
  .catch(console.error);
