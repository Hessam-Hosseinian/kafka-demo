# Kafka Visual Demo

یک دموی آموزشی و بصری برای نمایش زنده‌ی مسیر پیام از **Producer** به **Apache Kafka** و سپس Consumerهای یک **Consumer Group**.

هدف پروژه این است که مفاهیم اصلی Kafka مثل Topic، Partition، Consumer Group و Rebalancing را با یک UI ساده و قابل مشاهده نمایش دهد.

## چیزی که دمو می‌بینید

- ارسال پیام تکی یا Batch از Producer
- Topic با نام `demo-packages`
- سه Partition
- سه Consumer در یک Consumer Group
- توزیع پیام‌ها بین Consumerها
- نمایش زنده‌ی پیام‌ها در React UI
- ارسال Eventهای وضعیت از Backend به Frontend با WebSocket
- Rebalancing هنگام قطع یا اتصال Consumer
- Load Test ساده برای مشاهده رفتار سیستم زیر بار

## Tech stack

- Apache Kafka
- Node.js
- Express 5
- KafkaJS
- WebSocket (`ws`)
- React 19
- Vite 7
- Docker Compose

## معماری

```text
React UI ──HTTP──> Express ──> Kafka Producer
    ▲                              │
    │                              ▼
    └──WebSocket── Express <── demo-packages
                                      │
                           3 Kafka partitions
                                      │
                         ┌────────────┼────────────┐
                         ▼            ▼            ▼
                    Consumer A   Consumer B   Consumer C
                         └──── demo-consumer-group ────┘
```

### مفاهیم اصلی

- **Producer** پیام‌های جدید را داخل Kafka منتشر می‌کند.
- **Topic** جریان نام‌گذاری‌شده‌ی پیام‌هاست؛ این پروژه از `demo-packages` استفاده می‌کند.
- **Partition** امکان پردازش موازی یک Topic را فراهم می‌کند.
- **Consumer** پیام‌ها را دریافت و پردازش می‌کند.
- **Consumer Group** کار Partitionها را بین Consumerهای عضو تقسیم می‌کند.

در یک Consumer Group، هر Partition در هر لحظه فقط به یک Consumer واگذار می‌شود. با سه Partition و سه Consumer، معمولاً هر Consumer یک Partition می‌گیرد. اگر یکی از Consumerها قطع شود، Kafka assignmentها را دوباره توزیع می‌کند.

## پیش‌نیازها

- Docker و Docker Compose
- Node.js 20.19 یا جدیدتر
- npm
- Bash و `curl` برای Load Test

## اجرا

### 1. Kafka

از ریشه‌ی پروژه:

```bash
docker compose up -d
```

برای مشاهده‌ی وضعیت Kafka:

```bash
docker compose logs -f kafka
```

### 2. Backend

در یک Terminal جدید:

```bash
cd backend
npm install
npm run dev
```

Backend روی آدرس زیر اجرا می‌شود:

```text
http://localhost:3000
```

اگر Kafka هنوز آماده نباشد، Backend هر چند ثانیه دوباره برای اتصال تلاش می‌کند. Topic مورد نیاز نیز توسط پروژه ایجاد می‌شود.

### 3. Frontend

در Terminal دیگری:

```bash
cd frontend
npm install
npm run dev
```

سپس باز کنید:

```text
http://localhost:5173
```

## API

### ارسال یک پیام

```bash
curl -X POST http://localhost:3000/api/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"Hello Kafka"}'
```

### ارسال Batch

تعداد پیام می‌تواند بین ۱ تا ۱۰۰۰ باشد:

```bash
curl -X POST http://localhost:3000/api/messages/batch \
  -H "Content-Type: application/json" \
  -d '{"count":20}'
```

## Load Test

از ریشه‌ی پروژه:

```bash
chmod +x scripts/load-test.sh

./scripts/load-test.sh 20 0.5
./scripts/load-test.sh 100 0.05
./scripts/load-test.sh 500 0
```

آرگومان اول تعداد پیام‌ها و آرگومان دوم فاصله‌ی ارسال‌ها برحسب ثانیه است.

## توقف پروژه

Backend و Frontend را با `Ctrl+C` متوقف کنید و سپس:

```bash
docker compose down
```

این پروژه یک دمو است و Kafka storage آن برای نگه‌داری دائمی داده طراحی نشده است.

## هدف آموزشی

این ریپو عمداً معماری ساده‌ای دارد تا جریان داده و رفتار Kafka قابل مشاهده باشد و برای آموزش یا ضبط ویدئوی مرحله‌به‌مرحله مناسب بماند.
