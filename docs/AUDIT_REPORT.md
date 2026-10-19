# Báo cáo rà soát mức độ hoàn thiện

Tài liệu này đối chiếu yêu cầu của đề bài với mã nguồn **hiện tại**. Các kết quả chạy thử trong quá khứ được ghi riêng; chúng không thay thế việc kiểm chứng lại sau mỗi lần sửa mã.

## Đối chiếu yêu cầu và mã nguồn

| Hạng mục | Trạng thái hiện tại | Phần mã đảm nhận và điều cần lưu ý |
|---|---|---|
| Nguồn sự kiện | Đã triển khai | `src/producer.py` đọc năm cột CSV, chuyển thành JSON, dùng `visitorid` làm Kafka key, hỗ trợ `SKIP`, `LIMIT`, điều tiết tốc độ và đảo thứ tự theo nhóm có giới hạn. Producer chưa lưu vị trí đọc tự động. |
| Kafka | Đã cấu hình | `docker/docker-compose.yml` chạy Kafka 3.8 KRaft, tạo topic mặc định `retail-rocket-events` với ba phân vùng và một bản sao. Đây là cấu hình demo một broker. |
| Phân tích sự kiện | Đã triển khai | `transform_events` trong `src/streaming_etl.py` dùng schema tường minh, chuyển Unix milliseconds thành `event_time`, lọc bản ghi thiếu trường bắt buộc, loại sự kiện lạ và giao dịch thiếu mã. |
| Cửa sổ và điểm quan tâm | Đã triển khai | `score_windows` nhóm theo cửa sổ trượt, `visitorid`, `itemid`; mặc định cửa sổ 10 phút, trượt 5 phút, watermark 5 phút. Điểm `view=1`, `addtocart=5`, `transaction=10`. |
| Điều kiện gợi ý | Đã triển khai | Phải đạt ngưỡng điểm, có ít nhất một `addtocart` và chưa có `transaction` trong cửa sổ. `write_batch` còn đối chiếu lịch sử mua xuyên cửa sổ. |
| Sản phẩm ứng viên | Đã triển khai | `src/build_candidates.py` tạo bảng đồng xem từ tối đa 500.000 dòng đầu; quét toàn bộ CSV để biết các `itemid` và tạo ứng viên phổ biến dự phòng. Chất lượng gợi ý chưa được đo lường. |
| Lọc và xếp hạng gợi ý | Đã triển khai | `write_batch` nối bảng ứng viên, loại sản phẩm đã mua, xếp theo `candidate_score` và mã sản phẩm, lưu JSON cùng lịch sử chống lặp. Có thể không có gợi ý hợp lệ. |
| Phiên truy cập | Đã triển khai | `summarize_sessions` dùng khoảng không hoạt động 30 phút, tạo `session_id` từ `visitorid` và đầu phiên, ghi các phiên đã đóng theo watermark. Một khách có thể có nhiều phiên. |
| Đầu ra và khôi phục | Đã triển khai với giới hạn | Ba truy vấn chính có checkpoint riêng; bật `ARCHIVE_RAW_EVENTS=1` sẽ thêm truy vấn thứ tư lưu sự kiện Kafka thô. `intent`, `sessions`, `purchases`, lịch sử gợi ý và sự kiện thô ghi Parquet; gợi ý và thống kê batch ghi JSON. Listener còn ghi offset đầu vào của batch điểm. Tệp đầu ra và checkpoint không được commit nguyên tử cùng nhau. |
| Tài liệu và kiểm tra | Có | `README.md`, các tệp `docs/*.md`, notebook EDA, `src/inspect_batch.py` và `tests/test_streaming_etl.py`. Bộ kiểm tra hiện có bao phủ logic cơ bản, chưa là bộ E2E đầy đủ. |

## Những điểm đã được sửa so với bản đầu

Bản đầu đã có luồng CSV → Kafka → Spark nhưng còn thiếu điều kiện phải có thêm vào giỏ, chưa loại giao dịch đã mua ở cửa sổ khác và từng dùng chính sản phẩm đang quan tâm làm gợi ý. Phiên bản hiện tại bổ sung bảng ứng viên sản phẩm khác, lịch sử mua, lịch sử gợi ý, truy vấn phiên và console sink. Cấu trúc thư mục đã chuyển mã sang `src/`, cấu hình Docker sang `docker/`.

Thông tin này là lịch sử thay đổi, không phải tuyên bố rằng mọi kịch bản đã được kiểm chứng bằng chạy thử mới.

## Bằng chứng đã có và phạm vi của nó

- Trong quá trình phát triển trước đó, bộ kiểm tra Spark trong container được ghi nhận chạy thành công với **3 bài kiểm tra, 0 lỗi**. Kết quả chỉ bao phủ những phép kiểm tra có trong `tests/test_streaming_etl.py`.
- Một lần chạy tích hợp cô lập với topic Kafka tạm và sự kiện tự tạo đã ghi nhận đầu ra điểm, gợi ý, lịch sử mua, phiên và ba checkpoint. Đây là dữ liệu tự tạo, không phải kết quả đo chất lượng trên toàn bộ Retail Rocket.
- Một lần chạy Compose với 1.000 sự kiện đầu cũng đã được quan sát trong quá trình làm dự án: producer hoàn tất, Spark ghi kết quả và có gợi ý/phiên. Con số đầu ra phụ thuộc cách chia micro-batch, nên không dùng làm kết quả cố định của mọi lần chạy.
- Lần chạy sạch với `SKIP=0`, `LIMIT=500`, `ARCHIVE_RAW_EVENTS=1` tại `output_500_clean2/` đã ghi nhận producer xác nhận đủ 500 sự kiện, Kafka có ba phân vùng, tổng `num_input_rows` của 12 batch điểm bằng 500 (bắt đầu từ `batch_id=0`), kho thô có 500 Kafka offset duy nhất khớp 500 dòng đầu CSV, và 13 dòng gợi ý. Số batch và gợi ý phụ thuộc cách chia micro-batch, không phải kết quả cố định.
- Công cụ `src/inspect_batch.py` đã chạy thực tế với batch điểm 1: manifest báo 54 sự kiện và truy được các bản ghi trong kho thô qua khoảng offset của ba phân vùng. Batch ID của truy vấn lưu thô độc lập với batch ID truy vấn điểm.
- Cấu hình Compose đã qua `docker compose config --quiet`; bộ kiểm tra Spark chạy lại trong container với **3 bài kiểm tra, 0 lỗi**.
- Với mặc định phiên 30 phút và watermark 5 phút, 500 dòng đầu chưa làm đóng phiên nào, nên `sessions/` rỗng ở lần chạy này. Bằng chứng phiên đã ghi ra thuộc lần chạy 1.000 dòng và kịch bản tích hợp cô lập trước đó; chưa kiểm tra lại phiên trên lần chạy 500 dòng.

## Giới hạn còn lại

1. Kafka chỉ có một broker; Spark chạy `local[2]`. Chưa có kiểm chứng khả năng chịu lỗi như một cụm production.
2. Producer không có checkpoint CSV hay mã sự kiện để chống phát trùng khi chạy lại. Kafka idempotence chỉ giúp giảm gửi trùng do retry của cùng producer.
3. Gợi ý dựa trên bảng tĩnh; nếu dùng cùng khoảng thời gian để tạo ứng viên và đánh giá thì có nguy cơ rò rỉ thông tin tương lai.
4. Sự kiện quá muộn có thể bị watermark loại. Gợi ý đã ghi không được thu hồi khi giao dịch đến sau.
5. Lịch sử mua và gợi ý được đọc lại từ các tệp batch, làm chi phí tăng theo số batch. Ghi tệp và checkpoint không tạo bảo đảm exactly once cho toàn bộ pipeline.
6. Chưa có kiểm chứng đầy đủ cho mọi kịch bản đến muộn, khởi động lại, hỏng giữa chừng và khôi phục sau thay đổi lược đồ.
7. `micro_batches/` ghi số dòng tổng hợp được cập nhật và thời gian callback, còn số sự kiện Kafka đầu vào nằm riêng trong `intent_input_offsets/`; đây không phải phép đo độ trễ đầu cuối.
8. Các tệp nguồn và tài liệu mới đang ở trạng thái chưa commit trong Git. Cần rà soát, commit và đẩy lên kho từ xa trước khi nộp liên kết Git. Thư mục output mẫu đã được loại khỏi Git để tránh đưa dữ liệu phát sinh vào commit.

Xem [hướng dẫn chạy](../README.md), [giải thích pipeline](PIPELINE_FLOW.md) và [kịch bản kiểm chứng](DEMO_SCENARIOS.md).
