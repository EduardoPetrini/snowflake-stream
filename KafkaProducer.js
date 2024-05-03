const { Kafka } = require('kafkajs');

class KafkaProducer {
  constructor(producer, topic) {
    this.producer = producer;
    this.topic = topic;
  }

  static async init({ topic }) {
    const kafka = new Kafka({
      clientId: 'local',
      brokers: ['localhost:9092'],
    });

    const producer = kafka.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000,
    });

    await producer.connect();

    return new KafkaProducer(producer, topic);
  }

  async getProducer() {
    if (this.producer) {
      return this.producer;
    }

    return this.init();
  }

  async push(message) {
    return await this.producer.send({
      topic: this.topic,
      messages: [message],
    });
  }
}

module.exports = KafkaProducer;
