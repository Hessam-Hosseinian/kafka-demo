import { useEffect, useRef, useState } from "react";

const API_URL = "http://localhost:3000";
const WS_URL = "ws://localhost:3000";
const consumerNames = ["Consumer A", "Consumer B", "Consumer C"];

const emptyConsumers = Object.fromEntries(
  consumerNames.map((name) => [
    name,
    { count: 0, lastMessage: "—", partition: "—", active: false },
  ]),
);

function Character({ color }) {
  return (
    <div className="character" style={{ "--character-color": color }}>
      <span className="antenna" />
      <span className="eye eye-left" />
      <span className="eye eye-right" />
      <span className="smile" />
    </div>
  );
}

function App() {
  const sceneRef = useRef(null);
  const timersRef = useRef([]);
  const [content, setContent] = useState("Hello Kafka");
  const [socketConnected, setSocketConnected] = useState(false);
  const [kafkaReady, setKafkaReady] = useState(false);
  const [producedCount, setProducedCount] = useState(0);
  const [consumers, setConsumers] = useState(emptyConsumers);
  const [events, setEvents] = useState([]);
  const [packages, setPackages] = useState([]);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState("");

  const addEvent = (text) =>
    setEvents((current) => [{ id: crypto.randomUUID(), text }, ...current].slice(0, 5));

  const animatePackage = (messageId, sourceSelector, targetSelector, kind) => {
    const scene = sceneRef.current;
    const source = scene?.querySelector(sourceSelector);
    const target = scene?.querySelector(targetSelector);
    if (!scene || !source || !target) return;

    const sceneBox = scene.getBoundingClientRect();
    const sourceBox = source.getBoundingClientRect();
    const targetBox = target.getBoundingClientRect();
    const id = crypto.randomUUID();
    const item = {
      id,
      label: messageId.replace("pkg-", "#"),
      kind,
      x: sourceBox.left + sourceBox.width / 2 - sceneBox.left,
      y: sourceBox.top + sourceBox.height / 2 - sceneBox.top,
      dx: targetBox.left + targetBox.width / 2 - sourceBox.left - sourceBox.width / 2,
      dy: targetBox.top + targetBox.height / 2 - sourceBox.top - sourceBox.height / 2,
    };

    setPackages((current) => [...current.slice(-29), item]);
    const timer = setTimeout(
      () => setPackages((current) => current.filter((entry) => entry.id !== id)),
      1150,
    );
    timersRef.current.push(timer);
  };

  useEffect(() => {
    let socket;
    let reconnectTimer;
    let closedByApp = false;

    const connect = () => {
      socket = new WebSocket(WS_URL);
      socket.onopen = () => setSocketConnected(true);
      socket.onclose = () => {
        setSocketConnected(false);
        setKafkaReady(false);
        if (!closedByApp) reconnectTimer = setTimeout(connect, 2500);
      };
      socket.onerror = () => socket.close();
      socket.onmessage = ({ data }) => {
        const event = JSON.parse(data);

        if (event.type === "system-status") {
          setKafkaReady(event.data.kafkaReady);
          return;
        }

        if (event.type === "message-produced") {
          setProducedCount((count) => count + 1);
          addEvent(`Producer sent ${event.data.messageId}`);
          animatePackage(event.data.messageId, "[data-producer-anchor]", "[data-kafka-anchor]", "produced");
        }

        if (event.type === "message-consumed") {
          const item = event.data;
          setConsumers((current) => ({
            ...current,
            [item.consumerName]: {
              count: current[item.consumerName].count + 1,
              lastMessage: item.messageId,
              partition: item.partition,
              active: true,
            },
          }));
          addEvent(`${item.consumerName} consumed ${item.messageId} from Partition ${item.partition}`);
          animatePackage(
            item.messageId,
            `[data-partition="${item.partition}"]`,
            `[data-consumer="${item.consumerName}"]`,
            "consumed",
          );
          const timer = setTimeout(
            () =>
              setConsumers((current) => ({
                ...current,
                [item.consumerName]: { ...current[item.consumerName], active: false },
              })),
            650,
          );
          timersRef.current.push(timer);
        }
      };
    };

    connect();
    return () => {
      closedByApp = true;
      clearTimeout(reconnectTimer);
      timersRef.current.forEach(clearTimeout);
      socket?.close();
    };
  }, []);

  const post = async (path, body) => {
    setSending(true);
    setError("");
    try {
      const response = await fetch(`${API_URL}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || "Request failed");
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setSending(false);
    }
  };

  const sendOne = (event) => {
    event.preventDefault();
    if (content.trim()) post("/api/messages", { content: content.trim() });
  };

  return (
    <main className="app-shell">
      <header>
        <div>
          <p className="eyebrow">EVENT STREAMING, VISUALIZED</p>
          <h1>Kafka Package Station</h1>
        </div>
        <div className={`connection ${socketConnected && kafkaReady ? "online" : "offline"}`}>
          <span />
          {socketConnected ? (kafkaReady ? "Kafka connected" : "Waiting for Kafka") : "Reconnecting…"}
        </div>
      </header>

      <section className="scene" ref={sceneRef}>
        <article className="producer-card panel">
          <div className="card-heading">
            <div>
              <p className="section-label">START HERE</p>
              <h2>Producer</h2>
            </div>
            <span className="counter">{producedCount} sent</span>
          </div>
          <div className="producer-machine">
            <Character color="#ffad66" />
            <div className="machine-copy">
              <strong>Message Maker</strong>
              <small>packs data into events</small>
            </div>
            <span className="package-port" data-producer-anchor>✦</span>
          </div>
          <form onSubmit={sendOne}>
            <label htmlFor="message">Package content</label>
            <input
              id="message"
              value={content}
              maxLength="120"
              onChange={(event) => setContent(event.target.value)}
              placeholder="Type a message…"
            />
            <button className="primary-button" disabled={sending || !kafkaReady}>Send Message <span>→</span></button>
            <button
              className="secondary-button"
              type="button"
              disabled={sending || !kafkaReady}
              onClick={() => post("/api/messages/batch", { count: 10 })}
            >
              Send 10 Messages
            </button>
          </form>
          <p className="error-message">{error}</p>
        </article>

        <article className="kafka-card panel">
          <div className="kafka-title">
            <span className="kafka-mark">K</span>
            <div><p className="section-label">MESSAGE BROKER</p><h2>Apache Kafka</h2></div>
          </div>
          <div className="broker" data-kafka-anchor>
            <div className="broker-top"><i /><i /><i /><span>TOPIC: demo-packages</span></div>
            {[0, 1, 2].map((partition) => (
              <div className="partition" key={partition} data-partition={partition}>
                <span>Partition {partition}</span>
                <div className="belt-dots"><i /><i /><i /><i /><i /></div>
                <b>→</b>
              </div>
            ))}
          </div>
          <p className="broker-note">One topic · Three parallel lanes</p>
        </article>

        <section className="consumer-column">
          <div className="consumer-heading">
            <div><p className="section-label">SHARED GROUP</p><h2>Consumers</h2></div>
            <span>demo-consumer-group</span>
          </div>
          {consumerNames.map((name, index) => {
            const consumer = consumers[name];
            return (
              <article
                className={`consumer-card panel ${consumer.active ? "is-active" : ""}`}
                key={name}
                data-consumer={name}
              >
                <Character color={["#7ec8a4", "#9ea9e8", "#d6a4db"][index]} />
                <div className="consumer-info">
                  <div className="consumer-name"><h3>{name}</h3><span className={kafkaReady ? "ready" : ""}>{kafkaReady ? "Connected" : "Disconnected"}</span></div>
                  <div className="consumer-stats">
                    <span><b>{consumer.count}</b> received</span>
                    <span><b>{consumer.lastMessage}</b> last ID</span>
                    <span><b>{consumer.partition}</b> partition</span>
                  </div>
                </div>
              </article>
            );
          })}
        </section>

        <div className="package-layer" aria-hidden="true">
          {packages.map((item) => (
            <div
              className={`flying-package ${item.kind}`}
              key={item.id}
              style={{
                "--x": `${item.x}px`, "--y": `${item.y}px`,
                "--dx": `${item.dx}px`, "--dy": `${item.dy}px`,
              }}
            >
              <span>{item.label}</span><i />
            </div>
          ))}
        </div>
      </section>

      <section className="events-panel">
        <div className="events-title"><span>●</span><div><p>LIVE EVENTS</p><small>latest activity</small></div></div>
        <div className="event-list">
          {events.length ? events.map((event) => <p key={event.id}>{event.text}</p>) : <p className="empty-event">Messages will appear here in real time…</p>}
        </div>
      </section>
    </main>
  );
}

export default App;

