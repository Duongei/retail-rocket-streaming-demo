# Retail Rocket Streaming

Dự án mô phỏng luồng sự kiện thương mại điện tử từ dữ liệu Retail Rocket. Python producer đọc CSV theo từng dòng, chuyển các hành vi `view`, `addtocart`, `transaction` thành JSON và gửi vào Apache Kafka. Kafka UI dùng để quan sát topic, partitions và messages.

Phiên bản hiện tại triển khai Task 1–2: producer và môi trường Kafka. Phần xử lý PySpark của Task 3 chưa được tích hợp.

```text
CSV Retail Rocket → Python Producer → Kafka → Kafka UI
```

## Cấu trúc dự án

```text
retail-rocket-streaming-demo/
├── data/
│   └── events_sorted.csv   # Dữ liệu đầy đủ, tự đặt vào sau khi clone
├── producer.py            # Đọc CSV và gửi sự kiện vào Kafka
├── docker-compose.yml     # Kafka, tạo topic, Kafka UI và producer
├── Dockerfile             # Môi trường Python và uv cho producer
├── pyproject.toml         # Khai báo dependency Python
├── uv.lock                # Khóa phiên bản dependency
├── .env.example           # Mẫu cấu hình tùy chọn
├── .dockerignore          # Loại dữ liệu/cache khỏi Docker build
├── .gitignore             # Loại dữ liệu, venv và cấu hình local khỏi Git
└── README.md
```

## Clone và chạy

### 1. Chuẩn bị

Cài Git và Docker Desktop, mở Docker Desktop và đợi Linux engine chạy. Lần đầu cần Internet để tải images và dependencies. Docker tự chuẩn bị môi trường; không cần cài Python, Java, uv hay thư viện Python trên host.

### 2. Clone repository

Clone repository và vào thư mục dự án:

```bash
git clone https://github.com/Duongei/retail-rocket-streaming-demo.git
cd retail-rocket-streaming-demo
```

### 3. Đặt dữ liệu

Tạo thư mục `data` và chép file `events_sorted.csv` do nhóm chia sẻ vào:

```text
retail-rocket-streaming-demo/data/events_sorted.csv
```

CSV đầy đủ không được lưu trên GitHub, nên cần thực hiện bước này trước khi chạy. File gồm năm cột: `timestamp`, `visitorid`, `event`, `itemid`, `transactionid`.

### 4. Khởi động demo

Chạy tại thư mục dự án, dùng cùng lệnh trên PowerShell, Linux hoặc macOS:

```bash
docker compose up --build -d
```

Docker sẽ chuẩn bị môi trường, khởi động Kafka, tạo topic `retail-rocket-events` với 3 partitions, bật Kafka UI và chạy producer. Mặc định producer đọc toàn bộ CSV với tốc độ replay 20x; dataset lớn nên quá trình gửi có thể kéo dài.

### 5. Theo dõi demo

Mở [Kafka UI](http://localhost:8085) → **local → Topics → retail-rocket-events → Messages**. Chọn đọc từ đầu (earliest/beginning) để xem dữ liệu đã gửi và tải lại để xem messages mới.

Xem tiến độ producer:

```bash
docker compose logs -f producer
```

`queued` là số records đã đưa vào client, `confirmed` là số Kafka xác nhận; `failed` là số gửi thất bại và `unresolved` là số đang chờ kết quả. Nhấn Ctrl+C để ngừng xem log; producer vẫn chạy.

### 6. Dừng demo

Dừng riêng producer, giữ Kafka và UI:

```bash
docker compose stop producer
```

Dừng toàn bộ hệ thống:

```bash
docker compose down
```

Dữ liệu Kafka vẫn được giữ trong Docker volume. Lần sau chạy lại producer sẽ đọc CSV từ đầu và có thể tạo messages trùng.
