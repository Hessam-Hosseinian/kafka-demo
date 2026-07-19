const { kafka, TOPIC } = require("./kafka");

const names = ["Consumer A", "Consumer B", "Consumer C"];
let consumers = [];
let onConsumed = () => {};

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function setConsumedHandler(handler) {
  onConsumed = handler;
}

async function startConsumers() {
  if (consumers.length) return;

  const createdConsumers = names.map(() =>
    kafka.consumer({ groupId: "demo-consumer-group" }),
  );

  try {
    await Promise.all(createdConsumers.map((consumer) => consumer.connect()));
    await Promise.all(
      createdConsumers.map((consumer) =>
        consumer.subscribe({ topic: TOPIC, fromBeginning: false }),
      ),
    );

    await Promise.all(
      createdConsumers.map((consumer, index) =>
        consumer.run({
          eachMessage: async ({ partition, message }) => {
            try {
              const packageMessage = JSON.parse(message.value.toString());
              const delay = 300 + Math.floor(Math.random() * 401);
              await sleep(delay);

              onConsumed({
                type: "message-consumed",
                data: {
                  consumerName: names[index],
                  messageId: packageMessage.id,
                  content: packageMessage.content,
                  partition,
                  offset: message.offset,
                  receivedAt: new Date().toISOString(),
                },
              });
            } catch (error) {
              console.error(`[${names[index]}] Could not process message:`, error.message);
            }
          },
        }),
      ),
    );

    consumers = createdConsumers;
  } catch (error) {
    await Promise.allSettled(createdConsumers.map((consumer) => consumer.disconnect()));
    throw error;
  }
}

async function stopConsumers() {
  const activeConsumers = consumers;
  consumers = [];
  await Promise.allSettled(activeConsumers.map((consumer) => consumer.disconnect()));
}

module.exports = { setConsumedHandler, startConsumers, stopConsumers };

