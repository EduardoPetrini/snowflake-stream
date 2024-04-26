const SnowflakeConnector = require('./SnowflakeConnector');

const { log } = console;

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

async function start() {
  const sfInstance = await SnowflakeConnector.getInstance();
  const dataStream = await sfInstance.getStreamData();

  const iterator = nextItem(dataStream);
  const { value: nextRow1 } = await iterator.next();
  console.log(nextRow1.C_CUSTKEY);

  const { value: nextRow2 } = await iterator.next();
  console.log(nextRow2.C_CUSTKEY);

  const { value: nextRow3 } = await iterator.next();
  console.log(nextRow3.C_CUSTKEY);

  const { value: nextRow4 } = await iterator.next();
  console.log(nextRow4.C_CUSTKEY);

  await new Promise(resolve => setTimeout(resolve, 10000));

  const { value: nextRow5 } = await iterator.next();
  console.log(nextRow5.C_CUSTKEY);

  await new Promise(resolve => setTimeout(resolve, 5000));
}

start()
  .then(() => console.log('Done'))
  .catch(console.error);
