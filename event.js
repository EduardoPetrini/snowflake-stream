const http = require('http');
const EventEmitter = require('events');
const SnowflakeConnector = require('./SnowflakeConnector');

const { log } = console;

const start = async () => {
  const sfInstance = await SnowflakeConnector.getInstance();
  const dataStream = await sfInstance.getStreamData();

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
