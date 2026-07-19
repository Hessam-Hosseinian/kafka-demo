# Kafka Visual Demo

یک دموی کوچک و آموزشی برای نمایش زنده مسیر پیام از **Producer** به **Kafka** و سپس یکی از سه **Consumer**. کدها عمداً کوتاه و بدون معماری یا ابزار اضافه نوشته شده‌اند تا برای تایپ مرحله‌به‌مرحله در ویدیو مناسب باشند.

## پیش‌نیازها

- Docker و Docker Compose
- Node.js 20.19 یا جدیدتر
- Bash و curl برای Load Test

## اجرا

۱. Kafka را اجرا کنید:

```bash
docker compose up -d
```

برای دیدن آماده‌شدن Kafka می‌توانید لاگ را دنبال کنید:

```bash
docker compose logs -f kafka
```

۲. در یک Terminal، Backend را اجرا کنید:

```bash
cd backend
npm install
npm run dev
```

Backend روی `http://localhost:3000` اجرا می‌شود. اگر Kafka هنوز آماده نباشد، Backend خاموش نمی‌شود و هر پنج ثانیه دوباره تلاش می‌کند. Topic به نام `demo-packages` با سه Partition نیز خودکار ساخته می‌شود.

۳. در Terminal دیگری، Frontend را اجرا کنید:

```bash
cd frontend
npm install
npm run dev
```

حالا `http://localhost:5173` را در مرورگر باز کنید. برای بهترین نمایش هنگام ضبط، از پنجره‌ای با نسبت 16:9 استفاده کنید.

## API

ارسال یک پیام:

```bash
curl -X POST http://localhost:3000/api/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"Hello Kafka"}'
```

ارسال یک Batch (از ۱ تا ۱۰۰۰ پیام):

```bash
curl -X POST http://localhost:3000/api/messages/batch \
  -H "Content-Type: application/json" \
  -d '{"count":20}'
```

## Load Test

از ریشه پروژه:

```bash
chmod +x scripts/load-test.sh
./scripts/load-test.sh 20 0.5
./scripts/load-test.sh 100 0.05
./scripts/load-test.sh 500 0
```

آرگومان اول تعداد پیام و آرگومان دوم فاصله ارسال‌ها برحسب ثانیه است. در انتها تعداد درخواست‌های موفق، ناموفق و زمان اجرا چاپ می‌شود.

## معماری ساده

```text
React UI ──HTTP──> Express ──> Kafka Producer
    ▲                              │
    │                              ▼
    └──WebSocket── Express <── demo-packages (3 partitions)
                                      │
                         ┌────────────┼────────────┐
                         ▼            ▼            ▼
                    Consumer A   Consumer B   Consumer C
                         └──── demo-consumer-group ────┘
```

- **Producer** رویدادهای جدید را تولید می‌کند.
- **Topic** جریان نام‌گذاری‌شده‌ای است که پیام‌ها در آن نگهداری می‌شوند؛ این پروژه از `demo-packages` استفاده می‌کند.
- **Partition** یک مسیر مستقل داخل Topic است. چند Partition امکان خواندن موازی پیام‌ها را فراهم می‌کنند.
- **Consumer** پیام‌ها را می‌خواند و پردازش می‌کند.
- **Consumer Group** گروهی از Consumerهاست که کار یک Topic را بین خود تقسیم می‌کنند.

هر Partition در یک Consumer Group در هر لحظه فقط به یک Consumer واگذار می‌شود. بنابراین با سه Partition و سه Consumer، معمولاً هر Consumer صاحب یک Partition می‌شود و پیام‌ها بین آن‌ها تقسیم می‌شوند. اگر Consumerی قطع شود، Kafka Partition آن را به یکی از Consumerهای باقی‌مانده واگذار می‌کند.

## توقف پروژه

Backend و Frontend را با `Ctrl+C` متوقف کنید و سپس:

```bash
docker compose down
```

این دمو داده Kafka را دائمی نمی‌کند؛ با ساخت دوباره کانتینر، جریان پیام‌ها از ابتدا شروع می‌شود.
# kafka-demo
