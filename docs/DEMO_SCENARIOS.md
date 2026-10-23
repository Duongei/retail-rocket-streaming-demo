# Kịch bản trình diễn và kiểm chứng

Các bảng dưới đây mô tả **kết quả mong đợi** từ mã hiện tại, không có nghĩa mọi trường hợp đã được chạy và xác nhận. Với các tình huống watermark, cần gửi sự kiện có `timestamp` điều khiển được và xem `eventTime.watermark` trong log `query_progress`; chờ theo đồng hồ máy không đủ để đẩy watermark.

## Khởi động một lần chạy mới

Từ thư mục gốc dự án, chỉ tạo `.env` nếu chưa có:

```bash
test -f .env || cp .env.example .env
```

Để chạy 1.000 sự kiện đầu ở tốc độ cố định 10 sự kiện/giây, đặt `SO_DONG=1000`, `SPEEDUP=0`, `RATE=10` trong `.env`. Script tự đặt `SKIP=0`, `LIMIT=SO_DONG`, tên project và thư mục output mới:

```bash
sh scripts/demo.sh start
sh scripts/demo.sh logs
```

Dịch vụ `candidate-init` bảo đảm `data/item_candidates.csv` có tối đa một ứng viên cho mỗi sản phẩm; tự tạo lại bảng cũ nhiều ứng viên. Có thể xem topic qua [Kafka UI](http://localhost:8085). Dùng `sh scripts/demo.sh status` để xem project và output của lần chạy mới nhất, `sh scripts/demo.sh count` để xem số sự kiện Spark đã đọc.

## Khi checkpoint cũ không tương thích

Nếu thay đổi lược đồ trạng thái và Spark báo lỗi như `InvalidUnsafeRowException`, dừng project cũ mà **không dùng `-v`**, rồi tạo lần chạy sạch mới bằng cùng `SO_DONG` trong `.env`:

```bash
sh scripts/demo.sh down
sh scripts/demo.sh start
```

Script luôn đặt `SKIP=0` cho lần chạy mới, đồng thời tạo project và output riêng. Xem [README](../README.md) nếu muốn chạy tiếp các đoạn CSV trên cùng project.

## Các trường hợp cần quan sát

| Số | Dữ liệu/tác động có kiểm soát | Kết quả mong đợi |
|---:|---|---|
| 1 | Khách V001 xem A ở hai batch nhưng cùng cửa sổ thời gian sự kiện. | Điểm cửa sổ cập nhật 1 → 2; chưa đủ điều kiện gợi ý. |
| 2 | V001 thêm A vào giỏ ở batch sau, vẫn trong cửa sổ đó. | Điểm thành 7; có thể ghi một sản phẩm ứng viên nếu còn ứng viên hợp lệ. |
| 3 | V002 thêm B vào giỏ; V003 chỉ xem C; V004 thêm rồi mua D. | Các cặp độc lập; V002 và V004 đủ điểm, V003 với một view chưa đủ. Năm view cũng đủ ngưỡng. |
| 4 | Một sự kiện lúc 12:07. | Thuộc cả cửa sổ 12:00–12:10 và 12:05–12:15. |
| 5 | Watermark hiệu lực ở 12:07, rồi gửi sự kiện lúc 12:06 cho một cửa sổ chưa kết thúc. | Cửa sổ vẫn có thể nhận cập nhật; xác nhận bằng log tiến độ thay vì suy từ thời gian chờ. |
| 6 | Đẩy watermark đi qua cuối cửa sổ W1, rồi gửi sự kiện 12:04 thuộc W1. | W1 không cập nhật sau khi trạng thái đã bị dọn. |
| 7 | Thêm F vào giỏ, sau đó mua F ở batch/cửa sổ khác. | Điểm tiếp tục tăng, nhưng lịch sử gợi ý chặn lặp bộ ba đã xuất; không kiểm tra đã mua. |
| 8 | Điểm thay đổi nhiều lần và có các cửa sổ chồng nhau. | Mỗi cặp khách/sản phẩm quan tâm/cửa sổ chỉ chọn tối đa một ứng viên trong một batch; lịch sử còn chặn lặp cùng bộ ba ở batch sau. |
| 9 | Không có thông điệp Kafka mới, hoặc một batch xử lý quá 5 giây. | Truy vấn vẫn hoạt động; `TRIGGER_SECONDS=5` là chu kỳ lập lịch, không phải thời hạn hoàn tất batch. |
| 10 | Dừng Spark rồi khởi động lại, giữ Kafka volume và các checkpoint. | Spark tiếp tục từ offsets/trạng thái đã lưu nếu cấu trúc truy vấn tương thích. Cần đối chiếu đầu ra để kiểm tra trường hợp lỗi giữa lúc ghi tệp. |
| 11 | Dùng sản phẩm có quan hệ đồng xem, sản phẩm chỉ có fallback, và ứng viên đã mua. | Tra ứng viên duy nhất kể cả đã mua; chỉ xuất nếu bộ ba chưa có trong lịch sử gợi ý. Sản phẩm ngoài CSV không có ứng viên. |
| 12 | Nhiều hành vi của cùng khách cách nhau dưới 30 phút, rồi một hành vi sau khoảng ngắt dài hơn. | Tạo hai phiên khác `session_id`; phiên đầu chỉ xuất ra khi watermark vượt `session_end`. |

Trong tình huống 5–6, W1 là cửa sổ 12:00–12:10. Để mô phỏng đến trễ khi phát CSV, đặt `REORDER_BUFFER=20`: producer trộn từng nhóm tối đa 20 dòng nhưng giữ nguyên `timestamp`. Điều này không bảo đảm tạo ra đúng một watermark mong muốn. Nếu cần sự kiện có thời điểm cụ thể, dừng producer tự động rồi gửi JSON bằng Kafka client hoặc `kafka-console-producer.sh`, đặt `timestamp` theo Unix milliseconds.

Bảng ứng viên dùng quan hệ đồng xem từ 500.000 dòng đầu; độ phổ biến dự phòng cũng tính trong phần này, còn các mã sản phẩm được quét từ toàn bộ CSV. Muốn tạo lại bảng, dừng Spark rồi chạy:

```bash
python3 src/build_candidates.py --input data/events_sorted.csv --output data/item_candidates.csv
```

Không diễn giải bảng ứng viên như mô hình đã được đánh giá chất lượng.
