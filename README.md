# Hướng dẫn sử dụng Kafka & Dự án Big Data

Dự án này bao gồm cấu hình cluster Kafka 3 broker chạy ở chế độ KRaft (không cần Zookeeper) và các công cụ quản lý đi kèm.

---

## 1. Cài đặt Môi trường Python
Để cài đặt các thư viện cần thiết cho lập trình Spark và Kafka:

1. **Tạo môi trường ảo:**
   ```powershell
   python -m venv venv
   ```
2. **Kích hoạt (PowerShell):**
   ```powershell
   .\venv\Scripts\activate
   ```
3. **Cài đặt thư viện:**
   ```powershell
   pip install -r requirements.txt
   ```

---

## 2. Quản lý Cluster Kafka (Docker)
Cluster bao gồm 3 broker: `broker-1`, `broker-2`, `broker-3`.

- **Khởi động cluster:**
  ```powershell
  docker compose up -d
  ```
- **Dừng cluster:**
  ```powershell
  docker compose down
  ```

---

## 3. Công cụ Quản lý Kafka nhanh
Bạn có 2 lựa chọn tùy theo môi trường đang sử dụng:

### Cách A: Dùng PowerShell (Khuyên dùng trên Windows)
Sử dụng file `kafka.ps1` có sẵn:

- **Liệt kê topic:** `.\kafka.ps1 list`
- **Tạo topic:** `.\kafka.ps1 create <tên_topic>`
- **Gửi tin nhắn:** `.\kafka.ps1 producer <tên_topic>`
- **Đọc tin nhắn:** `.\kafka.ps1 consumer <tên_topic>`
- **Kiểm tra trạng thái:** `.\kafka.ps1 status`

### Cách B: Dùng Makefile
Nếu máy bạn có cài đặt công cụ `make`:

- `make up`: Khởi động docker.
- `make kafka-list`: Liệt kê topic.
- `make kafka-create topic=my-topic`: Tạo topic.
- `make kafka-producer topic=my-topic`: Gửi tin nhắn.
- `make kafka-consumer topic=my-topic`: Đọc tin nhắn.

---

## 4. Địa chỉ Kết nối (Bootstrap Servers)
Khi lập trình Python (PySpark/Confluent-Kafka), hãy sử dụng:

| Môi trường | Địa chỉ kết nối |
| :--- | :--- |
| **Bên ngoài Docker (Host)** | `localhost:9092`, `localhost:9192`, `localhost:9292` |
| **Bên trong Docker (Shared Network)** | `broker-1:9094`, `broker-2:9094`, `broker-3:9094` |

*Ví dụ trong mã nguồn Python:*
```python
bootstrap_servers = "localhost:9092,localhost:9192,localhost:9292"
```

---

## 5. Lưu ý quan trọng
- File `requirements.txt` đã được sửa lỗi `jupyterlab` (trước đó là `jupyter-lab` dẫn đến lỗi cài đặt).
- Cấu hình KRaft sử dụng **Cluster ID** là `4L69HqvCRN6S3pD6shW6BA`. Nếu bạn xóa volumes và thay đổi ID này, tất cả các node phải được cập nhật đồng bộ.
