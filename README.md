# Hệ thống gợi ý sản phẩm theo hành vi người dùng với Kafka và Spark

Dự án mô phỏng luồng hành vi thương mại điện tử từ bộ dữ liệu Retail Rocket. Chương trình phát sự kiện từ CSV vào Kafka; Spark Structured Streaming tính điểm quan tâm, tạo gợi ý sản phẩm, nhận diện phiên truy cập và ghi kết quả để phân tích. Đây là bản demo chạy cục bộ phục vụ học tập về xử lý luồng, thời gian sự kiện, watermark và checkpoint.

## Mục lục

1. [Luồng xử lý và chức năng đã triển khai](#luồng-xử-lý-và-chức-năng-đã-triển-khai)
2. [Cấu trúc dự án](#cấu-trúc-dự-án)
3. [Dữ liệu và quy tắc xử lý](#dữ-liệu-và-quy-tắc-xử-lý)
4. [Hướng dẫn chạy dự án](#hướng-dẫn-chạy-dự-án)
5. [Cấu hình, kết quả và kiểm tra](#cấu-hình-kết-quả-và-kiểm-tra)
6. [Hạn chế và hướng phát triển](#hạn-chế-và-hướng-phát-triển)

## Luồng xử lý và chức năng đã triển khai

```text
data/events_sorted.csv → producer → Kafka (3 phân vùng) → Spark Structured Streaming
                    data/item_candidates.csv ────────────────┘
                                                       ├─ output/intent/          : điểm quan tâm (Parquet)
                                                       ├─ output/recommendations/ : gợi ý (JSON)
                                                       ├─ output/sessions/        : phiên đã đóng (Parquet)
                                                       ├─ output/purchases/       : lịch sử sản phẩm đã mua
                                                       ├─ output/recommendation_history/ : lịch sử gợi ý
                                                       ├─ output/raw_events/     : sự kiện Kafka thô (Parquet)
                                                       ├─ output/intent_input_offsets/ : offset đầu vào batch điểm
                                                       └─ output/micro_batches/   : thống kê batch
                                        Spark → console; ba checkpoint chính, một tùy chọn
```

Các chức năng hiện có:

- Phát lại hành vi `view`, `addtocart` và `transaction` từ CSV theo tốc độ cố định hoặc theo khoảng cách thời gian giữa sự kiện; có thể chọn vị trí bắt đầu và số dòng cần phát.
- Tạo chủ đề Kafka với ba phân vùng và xem thông điệp qua Kafka UI.
- Dùng thời gian sự kiện, watermark và cửa sổ trượt để tính điểm quan tâm cho từng cặp khách truy cập/sản phẩm.
- Đề xuất sản phẩm từ bảng ứng viên đồng xem và sản phẩm phổ biến; loại sản phẩm đã mua trước khi xếp hạng gợi ý.
- Gom sự kiện của từng `visitorid` thành phiên theo khoảng ngắt hoạt động; ghi phiên khi watermark cho phép đóng phiên.
- Ghi kết quả ra tệp, lưu checkpoint cho ba truy vấn chính; khi bật `ARCHIVE_RAW_EVENTS=1`, truy vấn thứ tư lưu sự kiện Kafka thô. Khoảng offset giúp tra lại đầu vào batch tính điểm.

Xem [báo cáo đối chiếu yêu cầu](docs/AUDIT_REPORT.md), [luồng xử lý chi tiết](docs/PIPELINE_FLOW.md) và [kịch bản demo](docs/DEMO_SCENARIOS.md).

## Cấu trúc dự án

| Đường dẫn                                      | Nội dung và nhiệm vụ                                                                                                             |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `README.md`                                      | Hướng dẫn tổng quan, vận hành và giới hạn của dự án.                                                                     |
| `.env.example`                                   | Giá trị cấu hình mẫu cho Docker Compose; sao chép thành `.env` để tùy chỉnh lần chạy.                                 |
| `docker/docker-compose.yml`                      | Khai báo Kafka, dịch vụ tạo topic, Kafka UI, producer, dịch vụ tạo ứng viên, Spark và các volume.                         |
| `docker/Dockerfile`                              | Tạo image Python chạy producer và cài các thư viện cần thiết.                                                               |
| `data/events_sorted.csv`                         | Dữ liệu đầu vào đã sắp theo thời gian; khoảng 2,75 triệu sự kiện Retail Rocket.                                         |
| `data/item_candidates.csv`                       | Bảng ứng viên gợi ý được tạo tự động nếu chưa có; không phải nhãn đánh giá chính xác của mô hình.          |
| `src/producer.py`                                | Đọc CSV, bỏ qua `SKIP` dòng, gửi tối đa `LIMIT` sự kiện lên Kafka và điều tiết tốc độ phát lại.               |
| `src/build_candidates.py`                        | Tạo các cặp sản phẩm đồng xem từ 500.000 dòng đầu, quét toàn bộ dữ liệu để bổ sung ứng viên dự phòng.         |
| `src/streaming_etl.py`                           | Đọc Kafka, tính điểm trong cửa sổ trượt, lọc và ghi gợi ý, theo dõi giao dịch, xác định phiên và ghi thống kê. |
| `src/inspect_batch.py`                           | Tìm các sự kiện Kafka thuộc một batch tính điểm bằng khoảng offset đã lưu.                                             |
| `scripts/demo.sh`                                | Đọc `SO_DONG` từ `.env`, tạo lần chạy mới và cung cấp lệnh xem log, đếm sự kiện, xem batch, chạy tiếp và dừng. |
| `src/pyproject.toml`, `uv.lock`                | Khai báo phụ thuộc và phiên bản gói Python cho môi trường phát triển.                                                    |
| `notebooks/retail_rocket_eda.ipynb`              | Phân tích dữ liệu đầu vào: sự kiện, khách truy cập, sản phẩm, phiên và các xu hướng cơ bản.                      |
| `notebooks/streaming_output_observability.ipynb` | Xem kết quả một lần chạy: số sự kiện, từng micro-batch, sự kiện thô, điểm, gợi ý, giao dịch và phiên.             |
| `tests/test_streaming_etl.py`                    | Các bài kiểm tra logic Spark; không tự chạy khi khởi động Compose.                                                          |
| `docs/AUDIT_REPORT.md`                           | Đối chiếu các yêu cầu của đề bài với phần triển khai và ghi nhận điểm còn thiếu.                                  |
| `docs/PIPELINE_FLOW.md`                          | Giải thích các nhánh xử lý, cửa sổ, watermark, checkpoint và ý nghĩa đầu ra.                                            |
| `docs/DEMO_SCENARIOS.md`                         | Những tình huống và lệnh dùng khi trình diễn pipeline.                                                                       |
| `docs/REPORT.md`                                 | Báo cáo thiết kế và cơ sở lý thuyết của dự án.                                                                           |
| `docs/presentation.html`                         | Trang trình chiếu có thể mở bằng trình duyệt.                                                                                |
| `docs/kafka_spark_streaming_subject (1).docx`    | Tài liệu đề bài gốc.                                                                                                           |
| `output/` hoặc `output_*/`                    | Kết quả sinh ra trong từng lần chạy; thư mục thực tế do `OUTPUT_HOST_DIR` chọn.                                          |
| Volume `kafka-data`, `spark-checkpoints`       | Dữ liệu Kafka và trạng thái Spark của một Compose project; không nằm trong thư mục output.                                |

Các thư mục kết quả như `output_500_demo/` là sản phẩm của một lần chạy cụ thể, không phải mã nguồn cần sửa. `data/item_candidates.csv` cũng là dữ liệu sinh ra; khi thay đổi thuật toán tạo ứng viên, cần dừng Spark, xóa hoặc tạo lại tệp này rồi khởi động Spark. Có thể tạo lại bằng lệnh `python3 src/build_candidates.py --input data/events_sorted.csv --output data/item_candidates.csv` nếu máy đã có Python 3.

## Dữ liệu và quy tắc xử lý

Mỗi dòng của `events_sorted.csv` có năm trường:

| Trường          | Ý nghĩa                                                                                                                                                                                               |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `timestamp`     | Thời điểm sự kiện theo Unix milliseconds; dữ liệu lịch sử thuộc năm 2015.                                                                                                                    |
| `visitorid`     | Mã khách truy cập do bộ dữ liệu cung cấp; cùng mã có thể xuất hiện ở nhiều phiên. Không có bằng chứng để đồng nhất mã này với danh tính khách hàng ngoài bộ dữ liệu. |
| `event`         | Loại hành vi: `view`, `addtocart` hoặc `transaction`.                                                                                                                                          |
| `itemid`        | Mã sản phẩm chịu tác động của hành vi.                                                                                                                                                         |
| `transactionid` | Mã giao dịch khi có mua hàng; thường trống ở các loại sự kiện khác.                                                                                                                        |

Ba khái niệm dễ nhầm:

- **Khách truy cập:** `visitorid` là định danh có sẵn trong dữ liệu và được giữ qua các sự kiện của khách đó.
- **Phiên:** Spark suy ra từ chuỗi sự kiện của cùng `visitorid`; khoảng cách không hoạt động `30 minutes` tạo ranh giới phiên. Một khách có thể có nhiều phiên.
- **Cửa sổ tính điểm:** cửa sổ trượt `10 minutes`, dịch mỗi `5 minutes`. Một sự kiện có thể đóng góp vào nhiều cửa sổ. Cửa sổ này không đồng nghĩa với phiên.

Điểm hành vi hiện tại: `view = 1`, `addtocart = 5`, `transaction = 10`. Một cặp khách/sản phẩm được xét gợi ý khi điểm đạt ngưỡng (mặc định `5`), đã có ít nhất một lần thêm vào giỏ và chưa quan sát thấy giao dịch cho cặp đó. Bảng `item_candidates.csv` chọn các sản phẩm liên quan theo đồng xem, kèm phương án dự phòng theo độ phổ biến. Nếu không còn ứng viên phù hợp, hệ thống không xuất gợi ý.

Watermark mặc định là `5 minutes` theo **thời gian sự kiện**. Sự kiện đến quá muộn có thể bị loại khỏi phép tổng hợp; phiên chỉ được ghi khi Spark thấy watermark đã đi qua cuối phiên. Vì vậy một phiên đang mở có thể chưa xuất hiện ngay trong `sessions/`, kể cả khi producer đã gửi xong dữ liệu.

## Hướng dẫn chạy dự án

Các lệnh dưới đây chạy từ **thư mục gốc dự án**. Mỗi lần dùng `start`, script tạo một lần chạy và một thư mục `output_...` mới; kết quả của những lần trước vẫn được giữ lại.

Nếu muốn chạy full file CSV thì set `SO_DONG=2756101` trong env.

### 1. Kiểm tra điều kiện cần thiết

- Mở Docker Desktop và bảo đảm có Docker Compose v2, khoảng 6 GB RAM khả dụng. Kiểm tra bằng `docker compose version`.
- Đặt dữ liệu tại `data/events_sorted.csv`. Kiểm tra bằng `ls -lh data/events_sorted.csv`.
- Tạo `.env` từ tệp mẫu nếu chưa có:

```bash
test -f .env || cp .env.example .env
```

Lần đầu chạy, Docker cần tải image và Spark Kafka connector. Pipeline chạy trong Docker, nên không cần cài Kafka, Spark hay Java trên máy. Lệnh `count` ở bước 2 cần Python 3 trên máy.

Chỉ một lần chạy có thể dùng các cổng `9092` và `8085` trên cùng máy. Trước khi dùng `start`, chạy `docker compose ls --all` để xem có project nào đang chạy hoặc đơn giản là vào Ứng Dụng Docker và dừng hoặc xoá luôn containers đang chạy đi là được. Nếu project đó được tạo bằng `sh scripts/demo.sh start`, dừng bằng `sh scripts/demo.sh down`. Nếu project được tạo bằng lệnh Compose thủ công hoặc script báo chưa có lần chạy nào (không có `.demo_last_run`), thay `TEN_PROJECT` bằng tên trong cột `NAME` rồi dừng bằng:

```bash
docker compose --env-file .env -p TEN_PROJECT -f docker/docker-compose.yml down
```

Lệnh `down` không xóa thư mục output. Không thêm `-v` nếu muốn giữ Kafka và checkpoint của lần chạy cũ.

### 2. Chạy từ đầu với 500 dòng

Mở `.env` và đặt `SO_DONG=500` và LIMIT=500, rồi chạy:

```bash
sh scripts/demo.sh start
sh scripts/demo.sh logs
```

Lệnh `start` in tên project và thư mục output mới. Lệnh `logs` theo dõi producer và Spark; nhấn `Ctrl+C` để thôi xem log, các container vẫn chạy. Có thể xem thông điệp ở [Kafka UI](http://localhost:8085), mục `local → Topics → retail-rocket-events → Messages`.

Sau khi producer gửi xong và Spark xử lý hết, kiểm tra:

```bash
sh scripts/demo.sh count
sh scripts/demo.sh status
```

Tổng sự kiện Kafka đã đọc sẽ là **500** và batch điểm đầu tiên có `batch_id=0`. Nếu tổng còn thấp hơn, chờ Spark xử lý rồi chạy lại `count`. Producer hiện `Exited (0)` sau khi gửi xong là bình thường; Spark vẫn chờ dữ liệu mới. Số batch và số gợi ý phụ thuộc tốc độ xử lý. Với `SESSION_GAP=30 minutes` và `WATERMARK=5 minutes` mặc định, `sessions/` có thể chưa có dữ liệu ở 500 dòng đầu.

### 3. Thay đổi số dòng và chạy lại từ đầu

Ví dụ, để chạy **1.000 dòng đầu** sau lần thử 500 dòng:

1. Dừng lần chạy hiện tại bằng `sh scripts/demo.sh down`.
2. Sửa **duy nhất** `SO_DONG=1000` trong `.env`.
3. Chạy `sh scripts/demo.sh start`, rồi `sh scripts/demo.sh logs` và `sh scripts/demo.sh count` như bước 2.

Để chạy 5.000 dòng đầu, làm tương tự với `SO_DONG=5000`. Không cần sửa `LIMIT`: lệnh `start` tự đặt `SKIP=0`, `LIMIT=SO_DONG` và tạo project/output mới. `down` không xóa kết quả; ví dụ, hai lần chạy sẽ để lại `output_500_.../` và `output_1000_.../` riêng biệt, kể cả khi cùng chạy trên một máy.

`sh scripts/demo.sh status` cho biết thư mục output của lần chạy gần nhất. Mở [notebook quan sát output](notebooks/streaming_output_observability.ipynb) và đặt `OUTPUT_FOLDER` thành tên thư mục đó; muốn xem lần cũ thì nhập tên thư mục cũ. Tệp `.demo_last_run` chỉ lưu thông tin lần chạy gần nhất.

### 4. Gửi tiếp các dòng kế tiếp vào cùng lần chạy

Nếu đã gửi 500 dòng đầu và muốn gửi **1.000 dòng tiếp theo** (dòng dữ liệu 501–1.500), giữ hệ thống đang chạy và dùng:

```bash
sh scripts/demo.sh append 500 1000
```

Số `500` là lượng dòng dữ liệu CSV cần bỏ qua; `1000` là lượng dòng gửi tiếp. Lệnh `append` dùng lại project, checkpoint và output hiện tại, nên batch ID sẽ tăng tiếp. Sau khi Spark xử lý xong, `sh scripts/demo.sh count` sẽ cho tổng **1.500** sự kiện đã đọc. Không chạy `down` trước `append`, và không lặp lại cùng lệnh `append 500 1000`: producer sẽ phát trùng đoạn dữ liệu đó. `SO_DONG` chỉ dùng cho `start`; hai số truyền vào `append` quyết định đoạn dữ liệu gửi tiếp.

## Cấu hình, kết quả và kiểm tra

Compose lấy cấu hình trong `.env` nhờ tham số `--env-file .env`. Script `demo.sh start` đọc `SO_DONG` từ tệp này và chủ động đặt `SKIP=0`, `LIMIT=SO_DONG`, thư mục output mới; các tham số còn lại vẫn lấy từ `.env`. Khi dùng Compose trực tiếp, biến đã `export` trong terminal sẽ ưu tiên hơn giá trị cùng tên trong `.env`:

| Biến                       |    Mặc định | Tác dụng                                                                                                                |
| --------------------------- | -------------: | ------------------------------------------------------------------------------------------------------------------------- |
| `SO_DONG`                 |        `500` | Số dòng đầu CSV cho `sh scripts/demo.sh start`; chỉ cần đổi giá trị này để chạy 1.000, 5.000 dòng đầu. |
| `LIMIT`                   |         trống | Số sự kiện tối đa producer gửi; trống là đến hết CSV.                                                          |
| `SKIP`                    |          `0` | Số dòng dữ liệu bỏ qua sau dòng tiêu đề.                                                                         |
| `SPEEDUP`                 |         `20` | Hệ số tăng tốc theo khoảng thời gian sự kiện; `0` chuyển sang tốc độ cố định.                            |
| `RATE`                    |         `10` | Số sự kiện/giây khi `SPEEDUP=0`.                                                                                    |
| `MAX_SLEEP`               |          `5` | Thời gian chờ tối đa giữa hai sự kiện.                                                                             |
| `REORDER_BUFFER`          |          `0` | Kích thước bộ đệm đảo thứ tự để mô phỏng sự kiện đến trễ; `0` giữ thứ tự gốc.                    |
| `STARTING_OFFSETS`        |   `earliest` | Offset Kafka ban đầu khi Spark chưa có checkpoint.                                                                    |
| `WINDOW_DURATION`         | `10 minutes` | Độ dài cửa sổ tính điểm.                                                                                          |
| `SLIDE_DURATION`          |  `5 minutes` | Bước trượt của cửa sổ.                                                                                             |
| `WATERMARK`               |  `5 minutes` | Ngưỡng trễ theo thời gian sự kiện.                                                                                  |
| `SESSION_GAP`             | `30 minutes` | Khoảng không hoạt động phân tách phiên.                                                                           |
| `INTENT_THRESHOLD`        |          `5` | Điểm tối thiểu để xét gợi ý.                                                                                     |
| `TRIGGER_SECONDS`         |          `5` | Chu kỳ kích hoạt micro-batch Spark.                                                                                    |
| `MAX_OFFSETS_PER_TRIGGER` |          `0` | Giới hạn thông điệp Kafka mỗi batch; `0` là không giới hạn.                                                   |
| `OUTPUT_HOST_DIR`         |  `../output` | Thư mục kết quả trên máy, tính từ thư mục `docker/`.                                                          |
| `ARCHIVE_RAW_EVENTS`      |          `1` | `1` lưu toàn bộ sự kiện Kafka thô; `0` tắt truy vấn lưu thô để giảm đọc Kafka và dung lượng đĩa.  |

Trong thư mục output đã chọn:

| Thư mục                   | Nội dung cần hiểu                                                                                                             |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `intent/`                 | Các bản cập nhật điểm quan tâm theo cửa sổ; không phải một bảng trạng thái cuối cùng đã được gộp.         |
| `recommendations/`        | Các bản ghi gợi ý JSON; Spark cũng ghi gợi ý ra console.                                                                  |
| `sessions/`               | Các phiên đã đóng theo watermark, gồm thời điểm và các thống kê phiên.                                            |
| `purchases/`              | Lịch sử cặp khách truy cập/sản phẩm đã mua để lọc gợi ý ở các batch sau.                                         |
| `recommendation_history/` | Lịch sử gợi ý đã xuất để hạn chế gợi ý lặp.                                                                        |
| `micro_batches/`          | Thống kê xử lý từng batch; số dòng tổng hợp được cập nhật không đồng nghĩa số thông điệp Kafka đầu vào. |
| `raw_events/`             | Mọi bản ghi Kafka thô, gồm JSON gốc, partition, offset và `batch_id` của truy vấn lưu thô.                           |
| `intent_input_offsets/`   | Khoảng Kafka offset và số bản ghi đầu vào của từng batch tính điểm.                                                  |

### Xem kết quả bằng notebook

Mở [notebook quan sát output](notebooks/streaming_output_observability.ipynb) bằng Jupyter hoặc VS Code. Môi trường Python của notebook cần `pandas`, `pyarrow`, `matplotlib`, `ipykernel` và `jupyter`; nếu thiếu, cài bằng `python3 -m pip install pandas pyarrow matplotlib ipykernel jupyter` trong chính môi trường đó.

Chọn *streaming_output_observability.ipynb*  rồi run

Ở cell cấu hình đầu tiên, chỉ đổi `OUTPUT_FOLDER = "..."` thành tên thư mục output cần xem, rồi chọn **Run All**. Notebook hiển thị số bản ghi Kafka đã vào truy vấn điểm, số batch, bản cập nhật cửa sổ, gợi ý, phiên đã đóng, bảng theo từng batch và mẫu sự kiện thô. Ở cell cuối, đổi `SELECTED_BATCH_ID` để xem toàn bộ thông tin của một batch điểm. Nếu Spark vẫn đang chạy, chạy lại từ cell **Nạp ảnh chụp output** để cập nhật kết quả.

Số batch của truy vấn lưu thô khác số batch của truy vấn điểm; notebook nối sự kiện bằng topic, partition và offset. Các lần chạy không bật `ARCHIVE_RAW_EVENTS=1` hoặc thiếu `intent_input_offsets/` vẫn xem được phần tổng hợp nhưng không thể liệt kê chính xác sự kiện Kafka của từng batch điểm. Notebook chỉ đọc output đã ghi, không thể đếm những lần trigger khi Spark rảnh và không tạo batch.

### Xem sự kiện đầu vào của một batch tính điểm

Với lần chạy mới nhất, xem các ID trong thư mục `intent_input_offsets/` của output được in bởi `sh scripts/demo.sh status`, rồi chọn một ID để truy các sự kiện đầu vào. Ví dụ nếu có `batch_id=1.json`:

```bash
sh scripts/demo.sh batch 1 500
```

Số đầu tiên là `batch_id` của truy vấn điểm; số thứ hai là số dòng tối đa hiển thị (mặc định 100 nếu bỏ qua). Công cụ lấy khoảng offset trong `intent_input_offsets/` rồi tìm bản ghi tương ứng trong `raw_events/`. Cột `batch_id` của `raw_events/` thuộc **truy vấn lưu thô**; nó không nhất thiết bằng `batch_id` của `intent/`. Nếu số bản ghi thô chưa bằng số Spark báo đã đọc, công cụ sẽ cảnh báo. Muốn lọc theo `visitorid`, dùng `src/inspect_batch.py` trực tiếp với tham số `--visitorid`.

Đầu ra này chỉ có ở những lần chạy đã bật lưu thô và ghi manifest offset. Các batch cũ thiếu một trong hai nguồn đó không thể tra ngược chỉ từ tệp output hiện có. Muốn thử tính năng, tạo lần chạy mới theo mục **3. Thay đổi số dòng và chạy lại từ đầu**. Script chỉ nhớ lần chạy mới nhất; với output cũ, dùng notebook bằng cách đặt `OUTPUT_FOLDER` trực tiếp thành tên thư mục đó.

Nếu muốn chạy bài kiểm tra Spark có sẵn, trước hết lấy tên project từ `sh scripts/demo.sh status`, rồi thay `TÊN_PROJECT` trong lệnh sau:

```bash
docker compose --env-file .env -p TÊN_PROJECT -f docker/docker-compose.yml run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-streaming \
  --master 'local[2]' /app/tests/test_streaming_etl.py
```

Dừng toàn bộ stack mới nhất:

```bash
sh scripts/demo.sh down
```

`down` giữ dữ liệu Kafka và checkpoint trong Docker volume. Chỉ thêm `-v` khi muốn xóa các volume của Compose project đó. Thư mục output trên máy là bind mount nên lệnh `down` không xóa nó.

## Hạn chế và hướng phát triển

### Hạn chế hiện tại

- Hệ thống dùng một Kafka broker, replication factor 1 và Spark chạy cục bộ; chưa có dự phòng lỗi hoặc khả năng mở rộng cho môi trường production.
- Producer đọc lại CSV theo `SKIP`/`LIMIT` và không có checkpoint vị trí hay mã sự kiện duy nhất để chống phát trùng giữa các lần chạy.
- Bảng ứng viên là quy tắc đồng xem và độ phổ biến tĩnh, không phải mô hình học máy. Việc dùng phần đầu của cùng bộ dữ liệu để xây bảng có thể gây rò rỉ thời gian nếu dùng kết quả để đánh giá gợi ý.
- Watermark có thể bỏ qua sự kiện đến quá muộn. Giao dịch mua đến sau khi đã xuất gợi ý không thể thu hồi bản ghi gợi ý cũ.
- Checkpoint Spark và các tệp đầu ra không được ghi trong một giao dịch chung; không nên xem toàn pipeline là exactly once từ đầu đến cuối.
- Việc đọc lại lịch sử giao dịch và gợi ý từ tệp ở mỗi batch, cùng các truy vấn Spark riêng, sẽ tốn tài nguyên khi dữ liệu tăng. Lịch sử gợi ý còn có thể ngăn lặp lại cùng bộ ba khách/sản phẩm quan tâm/sản phẩm đề xuất ở các phiên sau.
- Truy vấn lưu sự kiện thô đọc Kafka thêm một lượt và cần thêm dung lượng đĩa. Khoảng offset của batch điểm được ghi sau khi Spark báo tiến độ; nếu chưa thấy tệp offset hoặc bản ghi thô, cần chờ truy vấn lưu thô theo kịp hoặc kiểm tra log.
- `visitorid` chỉ là mã trong bộ dữ liệu; dự án chưa có đăng nhập, hồ sơ khách hàng hay cách nối danh tính giữa nhiều thiết bị.

### Hướng phát triển

1. Bổ sung `event_id` và checkpoint cho producer; thiết kế đầu ra có khóa chống trùng và ghi nhất quán khi khởi động lại.
2. Chuyển lịch sử giao dịch và gợi ý sang kho dữ liệu hoặc state store phù hợp để tránh quét toàn bộ tệp mỗi batch.
3. Tạo ứng viên từ dữ liệu quá khứ theo mốc thời gian rõ ràng, đánh giá chất lượng gợi ý trên tập tương lai và thử thêm mô hình xếp hạng.
4. Xây API hoặc dashboard đọc dữ liệu đã chuẩn hóa; bổ sung chỉ số theo dõi độ trễ, throughput và chất lượng gợi ý.
5. Thử triển khai nhiều broker và Spark cluster với lưu trữ checkpoint bền vững, sau đó kiểm tra khả năng khôi phục khi có lỗi.

Trước khi chia sẻ dự án qua Git, cần đưa các mã nguồn và tài liệu mới trong `src/`, `docker/`, `docs/`, `notebooks/`, `tests/` vào commit. Không đưa `.env` hay thư mục output của từng lần chạy vào commit.
