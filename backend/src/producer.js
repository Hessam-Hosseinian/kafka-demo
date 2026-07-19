const crypto = require("crypto");
const { kafka, TOPIC } = require("./kafka");

const producer = kafka.producer();
let connected = false;
let onProduced = () => {};

function setProducedHandler(handler) {
  onProduced = handler;
}

async function connectProducer() {
  if (connected) return;
  await producer.connect();
  connected = true;
}

async function sendPackage(content) {
  if (!connected) throw new Error("Kafka producer is not connected");

  const packageMessage = {
    id: `pkg-${crypto.randomUUID().slice(0, 8)}`,
    content,
    createdAt: new Date().toISOString(),
    producerName: "Demo Producer",
  };

  await producer.send({
    topic: TOPIC,
    messages: [{ value: JSON.stringify(packageMessage) }],
  });

  onProduced({
    type: "message-produced",
    data: {
      messageId: packageMessage.id,
      content: packageMessage.content,
      createdAt: packageMessage.createdAt,
    },
  });

  return packageMessage;
}

async function disconnectProducer() {
  if (!connected) return;
  await producer.disconnect();
  connected = false;
}

module.exports = {
  connectProducer,
  disconnectProducer,
  sendPackage,
  setProducedHandler,
};

