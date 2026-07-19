const http = require("http");
const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const { ensureTopic } = require("./kafka");
const {
  connectProducer,
  disconnectProducer,
  sendPackage,
  setProducedHandler,
} = require("./producer");
const {
  setConsumedHandler,
  startConsumers,
  stopConsumers,
} = require("./consumers");

const PORT = process.env.PORT || 3000;
const app = express();
const server = http.createServer(app);
const webSocketServer = new WebSocketServer({ server });
const clients = new Set();

let kafkaReady = false;
let connecting = false;
let retryTimer;
let shuttingDown = false;

app.use(express.json({ limit: "20kb" }));
app.use((request, response, next) => {
  response.setHeader("Access-Control-Allow-Origin", "http://localhost:5173");
  response.setHeader("Access-Control-Allow-Headers", "Content-Type");
  response.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (request.method === "OPTIONS") return response.sendStatus(204);
  next();
});

function broadcast(event) {
  const payload = JSON.stringify(event);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function setKafkaStatus(ready) {
  kafkaReady = ready;
  broadcast({ type: "system-status", data: { kafkaReady: ready } });
}

setProducedHandler(broadcast);
setConsumedHandler(broadcast);

webSocketServer.on("connection", (socket) => {
  clients.add(socket);
  socket.send(JSON.stringify({ type: "system-status", data: { kafkaReady } }));
  socket.on("close", () => clients.delete(socket));
  socket.on("error", (error) => console.error("WebSocket error:", error.message));
});

app.get("/api/status", (request, response) => {
  response.json({ apiReady: true, kafkaReady });
});

app.post("/api/messages", async (request, response) => {
  const content = request.body?.content;
  if (typeof content !== "string" || !content.trim()) {
    return response.status(400).json({ error: "content must be a non-empty string" });
  }
  if (!kafkaReady) {
    return response.status(503).json({ error: "Kafka is not available yet" });
  }

  try {
    const message = await sendPackage(content.trim());
    response.status(201).json({ success: true, message });
  } catch (error) {
    console.error("Producer error:", error.message);
    setKafkaStatus(false);
    await Promise.allSettled([stopConsumers(), disconnectProducer()]);
    scheduleKafkaConnection();
    response.status(503).json({ error: "Could not send message to Kafka" });
  }
});

app.post("/api/messages/batch", async (request, response) => {
  const count = Number(request.body?.count);
  if (!Number.isInteger(count) || count < 1 || count > 1000) {
    return response.status(400).json({ error: "count must be an integer from 1 to 1000" });
  }
  if (!kafkaReady) {
    return response.status(503).json({ error: "Kafka is not available yet" });
  }

  try {
    const messages = [];
    for (let index = 1; index <= count; index += 1) {
      messages.push(await sendPackage(`Batch message ${index}`));
    }
    response.status(201).json({ success: true, count: messages.length, messages });
  } catch (error) {
    console.error("Batch producer error:", error.message);
    setKafkaStatus(false);
    await Promise.allSettled([stopConsumers(), disconnectProducer()]);
    scheduleKafkaConnection();
    response.status(503).json({ error: "Batch stopped because Kafka is unavailable" });
  }
});

async function connectToKafka() {
  if (connecting || kafkaReady || shuttingDown) return;
  connecting = true;

  try {
    await ensureTopic();
    await connectProducer();
    await startConsumers();
    setKafkaStatus(true);
    console.log("Kafka is connected; topic demo-packages has 3 partitions.");
  } catch (error) {
    console.error(`Kafka is unavailable: ${error.message}. Retrying in 5 seconds...`);
    await Promise.allSettled([stopConsumers(), disconnectProducer()]);
    setKafkaStatus(false);
    scheduleKafkaConnection();
  } finally {
    connecting = false;
  }
}

function scheduleKafkaConnection() {
  if (retryTimer || shuttingDown) return;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    connectToKafka();
  }, 5000);
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n${signal} received. Closing connections...`);
  clearTimeout(retryTimer);
  for (const client of clients) client.close();
  await Promise.allSettled([stopConsumers(), disconnectProducer()]);
  webSocketServer.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

server.listen(PORT, () => {
  console.log(`API and WebSocket are running on http://localhost:${PORT}`);
  connectToKafka();
});
