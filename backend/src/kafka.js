const { Kafka, logLevel } = require("kafkajs");

const TOPIC = "demo-packages";

const kafka = new Kafka({
  clientId: "kafka-visual-demo",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 5 },
});

async function ensureTopic() {
  const admin = kafka.admin();

  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
    });
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

module.exports = { kafka, TOPIC, ensureTopic };

