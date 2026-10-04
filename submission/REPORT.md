# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Ngo Tuan Tung / 2A202602826
**Repo:** [K4-Track02-Day17-Data-Pipeline-Engineering](https://github.com/tuantung26/K4-Track02-Day17-NgoTuanTung-2A202602826-DataPipelineEngineering)
**Commit bài nộp:** done
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE Agent hỗ trợ chạy code, fix lỗi theo yêu cầu của lab.
**Nguồn tham khảo khác (nếu có):** 

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | test_silver_tickets_one_row_per_ticket fail (24 != 12) | test_feature_daily_reconciles_with_full_recompute fail | test_deleted_ticket_leaves_training_and_rag fail |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` nên nó lưu lại mọi thay đổi thay vì đè trạng thái mới nhất. | `LOOKBACK_DAYS` = 0 nên pipeline không quét lùi về trước để đếm các sự kiện đến trễ. | Debezium delete có `after` là null, parser cũ bỏ qua `ticket_id` vì chỉ trích xuất từ `after`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Dùng `MERGE INTO` để upsert, điều kiện `s._lsn > coalesce(t._lsn, 0)`. | `pipeline/config.py`: Đặt `LOOKBACK_DAYS = 3`. | `pipeline/staging.py`: Dùng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id')`. |
| **Khái niệm trên slide** | Idempotency / Deduplication | Lateness / Late events handling | Tombstone / CDC |

## 2. Các con số

- P99 lateness đo từ Bronze: `3` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS / FAIL — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver chứa entities nên cần update tinh chỉnh theo khoá (SCD), còn gold_feature chứa aggregates tổng hợp theo ngày nên ghi đè theo partition (ngày) sẽ rẻ, an toàn và dễ bảo trì hơn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giúp lưu vết lịch sử biến đổi, đảm bảo hệ thống nhận diện được "đây là bản ghi đã xóa" và không "hồi sinh" nó nếu replay dữ liệu cũ.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Để đảm bảo reproducibility (kết quả huấn luyện model luôn lặp lại y nguyên khi dùng một phiên bản dữ liệu ở thời điểm quá khứ).
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu cỡ nhỏ đến vừa hoàn toàn có thể xử lý trong-tiến-trình (in-process) với DuckDB vô cùng nhanh gọn trên máy đơn mà không cần phải phân bổ hệ sinh thái phân tán cồng kềnh như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   **Trả lời:** Đối với snapshot "as of", chúng mang ý nghĩa lưu trữ vĩnh viễn (immutable). Tuy nhiên, về mặt tuân thủ luật (GDPR/Right to be Forgotten), chúng ta cần một quy trình ghi đè (masking/purging) định kỳ chạy trên chính cả snapshot cũ để xóa các thực thể nhạy cảm, đồng thời đảm bảo không có ai lấy tập dữ liệu cũ đó để tiếp tục huấn luyện model.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   **Trả lời:** Việc che tên người rất phức tạp do tính đa dạng, ta nên đặt thêm một chốt xử lý NLP (Named Entity Recognition) hoặc thư viện (như Microsoft Presidio) ở tầng Staging (ngay sau khi lấy từ Bronze) hoặc ngay trong hàm `mask_pii` của Silver. Việc đo đạc độ hiệu quả sẽ dùng metrics Precision/Recall thông qua tập test tự gán nhãn thủ công (Ground truth benchmark).

## 5. Output (dán nguyên văn)

```text
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build
RESULT: 18/18 checks — ALL PASS

# Lab 17 — re-run check for 2026-08-12
run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
