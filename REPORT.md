# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Lê Anh Duy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602723 |
| GitHub | Le-Anh-Duy |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-04324 |

## Kết quả cuối

- `dataset_version` của bạn: `e504467781ed`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 727 train / 117 val / 175 test (tổng cộng 1019 khung hình)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Quan sát đầu ra của lớp Bronze, telemetry giữa nguồn A (`raw_telemetry_a`) và nguồn B (`raw_telemetry_b`) có ít nhất 4 điểm khác biệt lớn:
1. **Tên cột và quy ước định danh**: Nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`. Nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`.
2. **Đơn vị đo lường**: Tốc độ nguồn A đo bằng mét/giây (`speed_mps`), nguồn B đo bằng km/giờ (`spd_kmh`). Bàn đạp ga/phanh ở A là tỷ lệ thập phân `0..1` (`gas`, `brake`), còn ở B là tỷ lệ phần trăm `0..100` (`throttle_pct`, `brake_pct`).
3. **Định dạng thời gian**: Nguồn A lưu chuỗi thời gian chuẩn ISO 8601 UTC dạng `YYYY-MM-DDTHH:MM:SS.sssZ` (ví dụ `2025-10-07T08:00:00.014Z`). Nguồn B lưu timestamp dạng số nguyên Unix epoch theo mili-giây (ví dụ `1759933801509`), đồng thời bị lệch đồng hồ 1500 ms (1.5 giây) so với thời điểm bắt đầu video.
4. **Tần số lấy mẫu và độ trễ**: Nguồn A có 3021 dòng dữ liệu, nguồn B có 3234 dòng dữ liệu dù cả hai video đều dài khoảng 5 phút, cho thấy tần số lấy mẫu giữa các cảm biến trên hai dòng xe không hoàn toàn đồng nhất.

Pipeline giữ **mỗi nguồn một bảng bronze riêng** và để **mọi cột ở dạng chuỗi (text/string)** vì:
- **Nguyên lý ghi nguyên bản (Raw fidelity & append-only)**: Lớp Bronze lưu trữ sự thật khách quan (source of truth). Nếu ép kiểu ngay lúc nạp, bất kỳ giá trị dị biệt nào (chuỗi lỗi, ô trống, dữ liệu biến dạng) sẽ gây lỗi crash pipeline nạp hoặc làm mất dữ liệu âm thầm.
- **Xử lý Schema Drift**: Mỗi xe hoặc nhà cung cấp cảm biến có thể thay đổi cấu trúc schema. Việc phân tách bảng giúp hệ thống mở rộng linh hoạt mà không phá vỡ cấu trúc hiện tại.
- **Khả năng tái xử lý (Replayability & Audit)**: Giữ nguyên văn dữ liệu thô kèm metadata nguồn (`_source_file`, `_batch_id`, `_row_no`, `_ingested_at`, `sha256`) giúp đội ngũ kỹ sư có thể audit vết dữ liệu và chạy lại các logic biến đổi ở tầng Silver khi quy tắc nghiệp vụ cập nhật.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Trong dbt (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`), pipeline chuẩn hoá hai nguồn về cùng một schema:
- **Đơn vị**: Thống nhất tốc độ về mét/giây (`m/s`). Nguồn A giữ nguyên `speed_mps`; nguồn B quy đổi từ `spd_kmh` sang `m/s` bằng phép chia `spd_kmh / 3.6`. Bàn đạp ga/phanh quy đổi về thang `0..1` (`throttle_pct / 100.0`). Hướng lái và tọa độ được map về cùng tên cột (`heading_deg`, `lat`, `lon`, `alt_m`).
- **Kiểu thời gian**: Nguồn A ép kiểu chuỗi `ts_utc` sang `TIMESTAMP WITH TIME ZONE (UTC)`. Nguồn B chuyển đổi số epoch mili-giây sang timestamp UTC (`to_timestamp(epoch_ms / 1000.0)`).
- **Độ lệch đồng hồ (clock offset)**: Nguồn B có độ lệch đồng hồ khai báo trong `sources.json` là `telemetry_clock_offset_ms = 1500` ms (1.5 giây). Mô hình dbt trừ trực tiếp 1500 ms khỏi timestamp của B (`epoch_ms - 1500`) trước khi tạo timestamp sự kiện.
- **Bằng chứng kết quả đúng**: Bảng kết quả Bước 2 cho thấy thời điểm bắt đầu của telemetry sạch Drive B là `2025-10-08 14:30:00.009000+00:00`, khớp hoàn toàn với thời điểm video B bắt đầu (`2025-10-08 14:30:00`), kiểm tra `drive B clock offset removed` đạt PASS. Tốc độ tối đa sau khi quy đổi của Drive A là 71.0 km/h (19.7 m/s) và Drive B là 100.0 km/h (27.8 m/s), hoàn toàn hợp lý với điều kiện giao thông thực tế.

Bảng dòng lỗi bị cách ly (Quarantine):
```
drive_id  reject_reason       so_dong
--------  ------------------  -------
drive_a   duplicate           20     
drive_a   speed_missing       30     
drive_a   speed_out_of_range  3      
drive_b   duplicate           20     
drive_b   speed_missing       30     
drive_b   speed_out_of_range  3      
```
Tổng cộng: 106 dòng bị cách ly (53 dòng ở Drive A và 53 dòng ở Drive B). Dòng sạch còn lại là 2968 (A) và 3181 (B); tổng dòng sạch + cách ly đúng bằng 3021 (A) và 3234 (B) từ nguồn thô.

Dòng lỗi được chuyển vào `silver.silver_telemetry_rejects` thay vì bị xoá thẳng vì:
- **Tránh mất mát thông tin âm thầm (No silent drops)**: Xoá dòng lỗi khiến đội vận hành không biết cảm biến bị hỏng, mạng truyền lỗi hay do logic lọc quá gắt.
- **Giám sát chất lượng dữ liệu (Data Quality Observability)**: Giữ lại dòng lỗi kèm cột `reject_reason` cho phép tính toán tỷ lệ lỗi (Error Rate SLA) theo từng xe/thiết bị, giúp phát hiện phần cứng xuống cấp.
- **Khả năng khắc phục hồi quy**: Nếu một quy tắc lọc sau này được xác định là quá chặt, các dòng trong quarantine có thể được phục hồi mà không cần nạp lại từ đầu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Sự phân biệt giữa mức độ cảnh báo (`warn`) và lỗi dừng hệ thống (`error`) trong kiểm thử dữ liệu:
- **`max_gap_ms` đặt là `warn`**: Kiểm tra khoảng cách thời gian tối đa giữa hai mẫu đo liên tiếp. Khi xe chạy qua khu vực khuất sóng, đường hầm hoặc cảm biến bị giật nhịp tạm thời, khoảng cách thời gian có thể giãn ra (trong bài là ~2.8 đến 3.1 giây). Dữ liệu này tuy bị gián đoạn nhưng không làm sai lệch bản chất các mẫu khác, vẫn dùng được cho nhiều bài toán không yêu cầu tần số cao. Do đó chỉ cần đưa ra cảnh báo (`warn`) để kỹ sư theo dõi mà không làm gián đoạn toàn bộ luồng pipeline.
- **`in_range` đặt là `error`**: Kiểm tra tốc độ phải nằm trong khoảng vật lý khả dĩ `0..70 m/s` (tương đương 0..252 km/h). Tốc độ âm hoặc tốc độ siêu thanh (>252 km/h) đối với xe ô tô dân dụng chắc chắn là dữ liệu hỏng hóc hoặc nhiễu loạn nghiêm trọng. Nếu dữ liệu này lọt vào tập huấn luyện của mô hình tự lái, nó sẽ làm hỏng hàm mất mát, gây nổ gradient hoặc khiến xe tự động ra quyết định nguy hiểm tới tính mạng. Do đó test này bắt buộc phải là `error` để ngắt pipeline ngay lập tức, ngăn rác lan sang Gold.

Kết quả thí nghiệm `break-gate` (`report/evidence/break_gate.json`):
- Khi nới luật làm sạch từ 70 lên 1000 m/s (cho phép tốc độ phi lý lọt qua bước lọc), lệnh `dbt test` cho kết quả: `PASS=5 WARN=1 ERROR=1 SKIP=0`.
- Test **`in_range_silver_telemetry_speed_mps__70__0`** đã phát hiện và chặn đứng chính xác **6 dòng** lỗi vượt ngưỡng tốc độ (mỗi drive có 3 dòng tốc độ dị biệt lọt qua).
- **Vai trò của cổng chất lượng (Quality Gate)**: Hoạt động như cầu chì tự động (circuit breaker / data firewall) bảo vệ toàn bộ hệ thống. Dù mã nguồn biến đổi ở thượng nguồn có bị sửa đổi sai hoặc lỗi nới lỏng quy tắc, cổng chất lượng sẽ ngăn chặn dứt khoát không cho dữ liệu độc hại lọt vào tầng Gold và Warehouse, bảo vệ độ tin cậy của mô hình AI.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào kết quả thí nghiệm `idempotency.json`:
- Trước khi chạy lại: bảng `silver.frames` có **1246 dòng**, mã băm dấu vân tay (fingerprint) là `6c7480dfde7e`.
- Sau 2 lần chạy lại liên tiếp cùng một partition (`trip-00003`): bảng vẫn giữ nguyên đúng **1246 dòng**, dấu vân tay vẫn là `6c7480dfde7e` (hoàn toàn **không đổi**).
- Thử nghiệm kiểu ghi sai (`append` hai lần cho một trip có 60 khung): số dòng của trip bị nhân đôi thành **120 dòng** (dữ liệu bị duplicate 2x).

Cơ chế Delta `overwrite` theo partition (dùng `mode="overwrite"` với predicate `trip_id = '<trip>'`) an toàn hơn hẳn việc "xoá rồi ghi lại" (DELETE rồi INSERT) vì:
1. **Tính nguyên tử của ACID Transaction**: Delta Lake sử dụng cơ chế transaction log (Delta Log / ARIES protocol). Khi overwrite theo partition, Delta Lake tạo các file Parquet mới cho partition đó và chỉ cập nhật con trỏ trong commit log khi thao tác hoàn tất thành công. Nếu quá trình trích xuất video hoặc tính toán bị sập/mất điện giữa chừng, transaction bị huỷ bỏ (aborted) và phiên bản dữ liệu cũ vẫn nguyên vẹn 100%. Không bao giờ xảy ra tình trạng bảng bị rỗng hoặc mất partition.
2. **Tránh mất dữ liệu khi lỗi**: Nếu dùng lệnh `DELETE` sau đó `INSERT`, nếu hệ thống sập ở thời điểm sau lệnh delete nhưng trước khi insert xong, toàn bộ dữ liệu của trip đó sẽ bị mất vĩnh viễn.
3. **Cách ly phân vùng (Partition Isolation)**: Việc ghi đè theo predicate chỉ tác động cục bộ lên duy nhất partition của trip đó mà không gây khóa bảng hay ảnh hưởng tới các worker khác đang xử lý 25 trip còn lại.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình **đen** và khung hình **tối**:
- **Khung đen (`is_black`)**: Độ sáng trung bình cực thấp (brightness < 5). Xảy ra do sự cố che khuất cảm biến, nắp ống kính đóng hoặc lỗi phần cứng camera mất tín hiệu hoàn toàn. Khung đen hoàn toàn không chứa thông tin hình ảnh thị giác hữu ích. Pipeline **gắn cờ và loại bỏ hoàn toàn** khỏi tập huấn luyện Gold (Drive A có 6 khung đen, Drive B có 4 khung đen).
- **Khung tối (`is_dark`)**: Độ sáng thấp (5 <= brightness < 30). Xảy ra khi xe di chuyển trong điều kiện ánh sáng yếu thực tế: chui qua hầm tối, đi dưới tán cây/bóng râm tòa nhà, hoặc lúc chập tối/ban đêm. Khung tối vẫn chứa đầy đủ cấu trúc hình học của làn đường, biển báo và phương tiện phía trước. Pipeline **gắn cờ nhận diện nhưng vẫn giữ lại (`dark_kept`)** cho tập Gold (Drive B có tới 61 khung tối được giữ lại, Drive A có 4 khung).
- **Đánh giá cá nhân**: Hoàn toàn đồng ý với thiết kế này. Xe tự hành bắt buộc phải có khả năng quan sát và điều khiển trong hầm và ban đêm. Nếu loại bỏ khung tối, mô hình sẽ bị thiên lệch (bias) chỉ biết lái lúc trời nắng và mất kiểm soát khi vào hầm tối.

Bảng số liệu độ nhạy từ `report/evidence/threshold.json`:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Phân tích và lựa chọn ngưỡng:
- **Lựa chọn ngưỡng 2** (ngưỡng mặc định của pipeline): Mã dHash 64-bit cho phép khác biệt tối đa 2 bit bắt được chính xác hiện tượng camera đứng hình (như đoạn camera bị đơ 6 giây ở Drive A phát hiện được 11/12 khung trùng) và khi xe đứng chờ đèn đỏ/chạy cực chậm, đồng thời vẫn bảo toàn các khung hình có chuyển động tự nhiên của môi trường.
- **Ngưỡng 0 quá chặt**: Chỉ bắt được các khung giống hệt nhau từng điểm ảnh pixel, dễ bỏ lọt các khung hình bị nhiễu hạt cảm biến hoặc ánh sáng dao động nhẹ dù xe đứng yên một chỗ.
- **Ngưỡng 6 quá rộng**: Loại bỏ tới 363 khung ở A và 244 khung ở B (hơn một nửa tập dữ liệu), vô tình xoá mất các khung hình có chuyển động xe bình thường.
- **Phụ thuộc vào tốc độ xe**: Khi xe chạy tốc độ cao, khoảng cách vật lý giữa hai lần lấy mẫu (cách nhau 0.5s) rất lớn, cảnh vật thay đổi mạnh khiến khoảng cách Hamming dHash lớn, ít bị nhận diện là trùng. Ngược lại, khi xe dừng đỗ hoặc di chuyển chậm, khoảng cách giữa 2 mẫu nhỏ nên dHash giữa các khung liền kề rất bé, số khung bị coi là trùng tăng lên.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào kết quả thí nghiệm `report/evidence/leakage.json`:
- **Chia theo khung hình (ngẫu nhiên 70/15/15)**: Có tới **26/26 trip** (100% các chuyến đi) bị chia cắt trên nhiều split, và **toàn bộ 26 trip đều có khung hình xuất hiện ở cả tập train lẫn tập test**.
- **Chia theo trip (cách chia của Gold pipeline)**: Có **0 trip** bị chia cắt, mỗi chuyến đi nằm trọn vẹn trong duy nhất một tập split.

Vì sao chia theo khung hình làm điểm kiểm thử "đẹp một cách giả tạo" (Data Leakage):
- Video trong bài được lấy mẫu ở tần số 2 fps (mỗi khung cách nhau chỉ 0.5 giây). Hai khung hình liên tiếp của cùng một chuyến đi có bối cảnh đường phố, thời tiết, ánh sáng, tòa nhà xung quanh và góc nhìn gần như tương đồng nhau (mức tương quan không gian - thời gian cực đại).
- Nếu chia theo khung hình, mô hình học khung hình ở giây $t$ trong tập train và được kiểm tra trên khung hình ở giây $t + 0.5$ trong tập test. Mô hình AI chỉ cần "học vẹt" (ghi nhớ) đặc trưng cảnh vật là có thể đạt độ chính xác kiểm thử (mAP, F1-score) cực cao và hàm loss rất nhỏ.
- Tuy nhiên, điểm kiểm thử cao này là ảo giác (leakage). Khi mô hình được đưa vào xe thực tế chạy trên một cung đường mới chưa từng thấy (unseen road / new trip), mô hình sẽ suy giảm hiệu năng nghiêm trọng vì không có khả năng tổng quát hóa (generalization). Chia theo cảnh/trip là tiêu chuẩn bắt buộc trong Computer Vision xe tự hành.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Tại sao `dataset_version` là băm nội dung (content hash, ví dụ `e504467781ed`):
- **Tính tất định và khả năng tái lập (Determinism & Reproducibility)**: Nếu định danh phiên bản theo ngày giờ (`2026-10-13_00:00`) hoặc số đếm tăng dần (`v1, v2`), cùng một bộ dữ liệu nếu chạy lại ở hai thời điểm khác nhau sẽ sinh ra hai mã phiên bản khác nhau. Điều này gây lãng phí bộ nhớ lưu trữ, trùng lặp dataset trong Data Lake và làm sai lệch việc theo dõi phiên bản trong MLOps. Mã băm nội dung đảm bảo: cùng nội dung thì luôn cùng mã phiên bản.
- **Kết quả hai lần chạy liên tiếp**: Cho ra đúng một chuỗi mã băm `dataset_version` giống hệt nhau (`e504467781ed`), đồng thời thư mục dataset không bị nhân đôi trong MinIO.

Vì sao `dataset_version` của tôi khác bạn bè:
- Trong mô hình `dbt/models/gold/gold_dataset_frames.sql`, thứ tự gán các trip vào từng tập split (train, val, test) được xáo trộn dựa trên hàm băm kết hợp với biến môi trường MSSV: `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`.
- Vì MSSV của tôi (`2A202602723`) là duy nhất và khác biệt với các bạn trong lớp, giá trị hash MD5 của từng trip sẽ khác, dẫn đến cách phân bổ các chuyến đi vào train/val/test khác nhau. Khi danh sách khung hình và file `manifest.json` có nội dung khác nhau, mã băm SHA256 tạo nên `dataset_version` sẽ hoàn toàn khác biệt giữa các sinh viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Trích xuất trực tiếp từ trường `lineage` trong `manifest.json` của phiên bản `e504467781ed`:
- **Video nguồn**: Tạo từ hai file video có mã băm SHA256 chính xác:
  * `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  * `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- **Phiên bản các bảng Delta thượng nguồn**:
  * `bronze.video_files`: commit version 1
  * `bronze.trips_raw`: commit version 0
  * `bronze.raw_telemetry_a`: commit version 0
  * `bronze.raw_telemetry_b`: commit version 0
  * `silver.frames`: commit version 27
  * `silver.telemetry`: commit version 0
  * `silver.frames_flagged`: commit version 0
  * `silver.frame_telemetry`: commit version 0
- **Quy tắc lọc và chia tập**:
  * `rules.drop`: loại bỏ các khung thoả mãn `["is_black", "is_blurry", "is_near_dup", "not sync_ok"]`.
  * `rules.split`: `per trip, stratified by drive, md5 order`.

Nếu một người hỏi sau 3 tháng để tái tạo lại chính xác 100% dataset này, cần cung cấp:
1. Hai video gốc có đúng mã SHA256 nêu trên và file telemetry gốc tương ứng.
2. Số phiên bản commit của các bảng Delta thượng nguồn để thực hiện Time Travel (đặc biệt là `silver.frames` ở version 27).
3. Giá trị salt ngẫu nhiên `LAB_STUDENT_ID` (`2A202602723`) và các luật lọc chất lượng/chia tập đã khai báo trong manifest.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip phân công của tôi là: **`trip-04324`** (tính từ `MSSV 2A202602723 % 26 = 11`).

Khi cấu hình `LAB_FAIL_ONCE=trip-04324` trong `.env` và kích hoạt DAG `day09_video_pipeline`:
1. **Diễn biến sập**: Trong nhánh 26 task song song của `silver_frames`, đúng **1 task** đại diện cho trip `trip-04324` bị crash và chuyển sang màu **ĐỎ (failed)** do mã nguồn kích hoạt ngoại lệ mô phỏng: `RuntimeError: simulated crash while extracting trip-04324`.
2. **Các task hạ nguồn**: 25 task trích xuất của các trip khác vẫn chạy thành công (xanh). Tuy nhiên, toàn bộ **7 task hạ nguồn** phụ thuộc phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) lập tức chuyển sang trạng thái **`upstream_failed` (màu cam)** và không được thực thi.
3. **Thao tác phục hồi**: Trên giao diện Airflow Graph/Grid, tôi chỉ cần chọn đúng task đỏ của `trip-04324` và nhấn **`Clear`** (chọn cờ downstream). Airflow kích hoạt lại chỉ duy nhất task này.
4. **Kết quả phục hồi**: Task `trip-04324` chạy lại thành công chuyển sang màu xanh, sau đó kéo theo toàn bộ 7 task hạ nguồn tiếp tục chạy và hoàn tất thành công (DAG chuyển toàn bộ sang màu **XANH**).
5. **Tính toàn vẹn dữ liệu**: Dữ liệu **hoàn toàn không bị nhân đôi** nhờ cơ chế Delta `overwrite` theo partition predicate `trip_id = 'trip-04324'` của hàm `extract_trip()`. Mã `dataset_version` vẫn giữ nguyên là **`e504467781ed`** (không thay đổi). Bằng chứng kiểm tra tự động `verify` vẫn đạt trọn vẹn **27/27 checks passed**.

Bằng chứng: Đã lưu ảnh chụp log lỗi của task đỏ tại `report/screenshots/trip_crash_recovery.png` và ảnh toàn bộ đồ thị DAG xanh tại `report/screenshots/airflow_graph.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng quy mô từ 2 chuyến xe lên 500 xe mỗi ngày (ước tính ~1500 giờ video, hàng triệu khung hình và hàng trăm GB dữ liệu mỗi ngày), 3 thành phần sẽ bị quá tải và đổ vỡ đầu tiên:

1. **Thành phần 1: Bộ giải mã video tuần tự bằng CPU/FFmpeg trên một Airflow Worker cục bộ (Slide 24)**
   - *Nguyên nhân sập*: Việc giải mã video là nút thắt cổ chai tính toán nặng nhất (decode bottleneck). Giải mã 1500 giờ video trên một tiến trình worker đơn lẻ sẽ ngốn sạch tài nguyên CPU/IO, gây tắc nghẽn hàng chục giờ và dẫn đến tràn đĩa đệm (Disk full / OOM crash).
   - *Giải pháp thay thế*: Tách tác vụ giải mã video ra khỏi Airflow core, chuyển sang một cụm xử lý video chuyên dụng phân tán (như cụm worker Ray / Spark hoặc Kubernetes Job) có hỗ trợ phần cứng **GPU/NVDEC Hardware Acceleration**. Chia video thành các chunk nhỏ (ví dụ 10–30 giây) để decode song song trên hàng trăm container, trích xuất đặc trưng (embeddings/dHash) và đẩy thẳng frame lên Object Storage (S3/GCS).

2. **Thành phần 2: Công cụ DuckDB chạy trên một nút đơn lẻ (Slide 16, 22)**
   - *Nguyên nhân sập*: DuckDB được thiết kế tuyệt vời cho phân tích in-process trên máy đơn (single-node). Nhưng với 500 xe/ngày, dữ liệu telemetry và frame metadata tăng lên hàng trăm triệu bản ghi mỗi ngày, DuckDB sẽ cạn kiệt RAM, chịu giới hạn ghi đơn luồng (single-writer constraint) và không thể mở rộng theo chiều ngang (scale-out).
   - *Giải pháp thay thế*: Thay thế DuckDB bằng các công cụ tính toán phân tán quy mô lớn như **Apache Spark (PySpark / Delta Lake)**, **Trino** hoặc nền tảng Data Lakehouse đám mây (**Databricks / Snowflake / BigQuery**). Các công cụ này cho phép chia nhỏ dữ liệu và thực thi dbt biến đổi song song trên một cụm gồm hàng chục worker node.

3. **Thành phần 3: Kiến trúc Airflow Dynamic Task Mapping với hàng nghìn task độc lập (Slide 26, 27)**
   - *Nguyên nhân sập*: Hiện tại pipeline tạo 1 task Airflow cho mỗi trip. Với 500 xe/ngày, số lượng trip có thể lên tới 5000–10000 trips. Việc tạo hàng vạn task run mỗi ngày sẽ làm tê liệt cơ sở dữ liệu metadata của Airflow (Postgres connection pool exhausted), scheduler bị nghẽn heartbeat, UI webserver bị treo/crash khi render đồ thị DAG, và chi phí quản lý task overhead vượt quá thời gian chạy thực tế.
   - *Giải pháp thay thế*: Thay đổi vai trò của Airflow theo đúng nguyên tắc: *Airflow chỉ điều phối cấp cao (Orchestration), không quản lý vi mô (Execution)*. Airflow chỉ trigger một vài pipeline batch job cấp cao (ví dụ: submit Spark/Ray batch job hoặc Argo Workflows). Việc chia nhỏ 5000 trip và quản lý retry phân tán sẽ do cluster scheduler chuyên trách tầng dưới đảm nhiệm.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
-- Câu hỏi: Phân tích tỷ lệ khung hình bị loại bỏ ở từng chuyến đi (Top 10 trip có tỷ lệ drop cao nhất)
select 
    t.trip_id,
    t.drive_id,
    t.total_frames,
    t.kept_frames,
    (t.total_frames - t.kept_frames) as dropped_frames,
    round(100.0 * (t.total_frames - t.kept_frames) / nullif(t.total_frames, 0), 1) as drop_rate_pct
from marts.mart_trip_summary t
order by drop_rate_pct desc
limit 10;
```

**Điều câu truy vấn này cho biết**:
Câu truy vấn tổng hợp và xếp hạng 10 chuyến đi có tỷ lệ khung hình bị loại bỏ cao nhất trong toàn bộ hệ thống. Trong dữ liệu thực tế, các chuyến đi như `trip-02086` và `trip-03904` của Drive A có tỷ lệ bị loại bỏ lên tới hơn 40% do camera gặp sự cố đứng hình (frozen camera) và xe dừng đỗ kéo dài (nhiều khung trùng). Thông tin này giúp đội ngũ vận hành hệ thống xe tự lái phát hiện nhanh các xe có thiết bị camera chập chờn, hoặc nhận diện các cung đường có điều kiện thời tiết/ánh sáng bất lợi để kịp thời bảo trì phần cứng và lập kế hoạch thu thập dữ liệu bổ sung.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Điểm bất ngờ nhất**: Khung hình tối (`is_dark`) trong môi trường thực tế (đường hầm, hoàng hôn) được gắn cờ nhưng giữ lại (`dark_kept`) chứ không bị xoá như khung đen (`is_black`), giúp mô hình tự lái học được khả năng quan sát thiếu sáng; cùng với cơ chế `overwrite` theo partition của Delta Lake bảo toàn trọn vẹn tính idempotent.
- **Điểm còn chưa rõ**: Khi quy mô tăng lên hàng trăm nghìn trip, cơ chế dọn dẹp các tệp Parquet rác của các phiên bản cũ trong Delta Lake (lệnh `VACUUM`) và tối ưu hóa layout tệp (`OPTIMIZE Z-ORDER`) cần được cấu hình lịch trình định kỳ ra sao để cân bằng giữa chi phí lưu trữ S3 và hiệu năng truy vấn.
- **Lỗi dữ liệu pipeline chưa bắt được**: Hiện tượng vết bùn bẩn, giọt nước mưa hoặc côn trùng bám cố định trên kính chắn gió che khuất đúng góc nhìn của biển báo giao thông; lúc này khung hình không đủ mờ để thành `is_blurry` và không đủ tối để thành `is_dark`, nhưng vẫn có thể làm mô hình nhận diện sai biển báo.
