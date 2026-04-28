# BÁO CÁO THỰC HÀNH BIG DATA (CO3137)
## LAB 2: SPARK STREAMING & KAFKA ADVANCED ANALYTICS

**Giảng viên hướng dẫn:** Đoàn Ngô Đức Phương  
**Lớp:** L01  
**Ngày thực hiện:** 14/04/2026

### Thông tin nhóm:
- **Lê Đình Đức** - 2310774
- **Nguyễn Văn Công Thành** - 2313133

---

## 1. TỔNG QUAN (OVERVIEW)

### 1.1 Apache Spark Streaming
Apache Spark Streaming là một thư viện mạnh mẽ được xây dựng trên Apache Spark SQL, cho phép xử lý dữ liệu streaming thời gian thực với các API DataFrame/Dataset quen thuộc. Các đặc điểm chính bao gồm:

- **Structured Streaming:** Áp dụng mô hình incremental computation, trong đó một truy vấn streaming được xem như một truy vấn batch chạy liên tục trên dữ liệu vô hạn (unbounded data). Spark chia dòng dữ liệu thành các micro-batches nhỏ để xử lý.
- **Exactly-once Semantics:** Đảm bảo tính toàn vẹn dữ liệu thông qua:
  - Checkpointing (lưu trạng thái xử lý)
  - Replay offsets (đối với Kafka, file source, v.v.)
  - Idempotent sink hoặc transactional sink
- **Windowing & Watermarking:** Hỗ trợ các loại cửa sổ (tumbling, hopping, sliding) và xử lý dữ liệu đến trễ (late-arriving data).

### 1.2 Watermark và Windowing
Trong streaming systems, dữ liệu thường không đến đúng thứ tự thời gian (out-of-order). Hai khái niệm quan trọng cần xử lý là:

- **Watermark:** Một cơ chế để Spark xử lý late-arriving data bằng cách đánh dấu dữ liệu đến trễ. Spark duy trì một mốc thời gian (event-time watermark) và các record có timestamp cũ hơn watermark sẽ bị loại bỏ.
- **Window:** Chia dòng dữ liệu streaming theo các khoảng thời gian để thực hiện aggregation:
  - **Tumbling Window:** Cửa sổ cố định, không chồng lấn, mỗi event chỉ thuộc một window duy nhất - lý tưởng cho các phép tính tổng hợp định kỳ.
  - **Hopping Window:** Chồng lấn, phù hợp cho các phép tính trung bình di động (moving average).
  - **Sliding Window:** Phản ứng theo sự thay đổi dữ liệu, phù hợp cho phân tích thời gian thực.

### 1.3 Output Modes
- **Append:** Chỉ xuất các hàng mới kể từ lần kích hoạt cuối (dùng cho các output không có aggregation hoặc stream-stream join).
- **Update:** Chỉ xuất các hàng thay đổi kể từ lần kích hoạt cuối (dùng cho các aggregation có state).
- **Complete:** Xuất toàn bộ bảng kết quả sau mỗi lần kích hoạt (dùng cho các aggregation tổng thể).

### 1.4 Stream-Stream Join
Một tính năng nâng cao cho phép kết hợp dữ liệu từ hai luồng streaming dựa trên một khóa (key) dùng chung. Cần sử dụng watermark trên cả hai stream để xử lý dữ liệu đến trễ và quản lý trạng thái hiệu quả.

---

## 2. TRIỂN KHAI CHI TIẾT (CODE & OUTPUTS)

**Yêu cầu cơ bản của bài tập:**
Bài tập này được thiết kế để giúp chúng em làm quen và thực hành các thao tác xử lý dữ liệu streaming trên Apache Spark Streaming kết hợp với Apache Kafka. Các yêu cầu cụ thể bao gồm:
- **Chuẩn bị dữ liệu (Exercise 1):** Đọc dữ liệu từ Kafka cho 3 topics (`movies`, `ratings`, `tags`). Định nghĩa Schema và parse dữ liệu dạng JSON thông qua luồng Structured Streaming.
- **Hot Genres (Exercise 2):** Thực hiện kết nối dòng dữ liệu đánh giá streaming với dữ liệu tĩnh movies (Static-Stream Join) để đếm số lượng đánh giá theo từng thể loại (genre).
- **Trending Now (Exercise 3):** Thao tác Windowed Aggregation với Tumbling Window 5 phút, đếm số lượng đánh giá theo bộ phim và dùng hàm window function `dense_rank` nhằm tìm top 3 bộ phim nổi bật trong mỗi khung giờ.
- **Stream-Stream Join (Exercise 4):** Phân tích tương quan theo thời gian thực giữa hai luồng streaming (`tags` và `ratings`), sử dụng cơ chế Watermark 10 phút để kiểm soát dữ liệu đến muộn.

### 2.1 Chuẩn bị dữ liệu và Kafka (Tương tự Lab 1)
Quá trình khởi tạo và đưa dữ liệu lên Kafka được thực hiện hoàn toàn tương tự như Lab 1, bao gồm các bước:
1. **Khởi tạo Spark Session:** Cấu hình phiên làm việc Spark với package `spark-sql-kafka-0-10`.
2. **Chuẩn bị dữ liệu:** Tải dataset `movielens-latest-small` từ Kaggle và nạp vào Spark DataFrame.
3. **Thiết lập Kafka Topics:** Khởi tạo tập tin 3 topics là `movies`, `ratings`, `tags` bằng `AdminClient` tại `localhost`.
4. **Đưa dữ liệu lên Kafka:** Chuyển đổi DataFrame sang định dạng String JSON và gửi lên Kafka thông qua `.write.format("kafka")`.
*(Mã nguồn chi tiết và kết quả thực thi của các bước này hoàn toàn tương tự với báo cáo Lab 1).*

### 2.2 Định nghĩa Schema và Đọc từ Kafka (Exercise 1)
Chúng em quy ước lại `StructType` Schema cho từng topic từ ban đầu và thiết lập hàm đọc dữ liệu theo luồng từ Kafka qua `readStream`. Đối với luồng `ratings`, nhằm tạo ra các micro-batches xử lý streaming kiểm soát nhịp độ, chúng em sử dụng kèm option `maxOffsetsPerTrigger=100`.

**Mã nguồn:**
```python
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def read_and_parse_kafka(topic, schema, max_offsets=None):
    """Read from Kafka and parse JSON string"""
    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic)
    
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    
    df_raw = reader.option("startingOffsets", "earliest").load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    return df_parsed

movie_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

rating_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

tag_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", IntegerType(), True)
])

df_movies_stream = read_and_parse_kafka("movies", movie_schema)
df_ratings_stream = read_and_parse_kafka("ratings", rating_schema, max_offsets=100)
df_tags_stream = read_and_parse_kafka("tags", tag_schema)

print("Movies DataFrame:")
df_movies_stream.printSchema()
print("Ratings DataFrame:")
df_ratings_stream.printSchema()
print("Tags DataFrame:")
df_tags_stream.printSchema()
```

**Kết quả thực thi:**
```text
Movies DataFrame:
root
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- genres: string (nullable = true)

Ratings DataFrame:
root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: integer (nullable = true)

Tags DataFrame:
root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- tag: string (nullable = true)
 |-- timestamp: integer (nullable = true)
```

### 2.3 Ghi Dữ Liệu Streaming vào JSON Files
Chúng em thu thập luồng dữ liệu thô vào các file JSON nội bộ làm kho dữ liệu dữ liệu Batch xử lý tĩnh. Giả lập một tiến trình Data Lake đơn giản.

**Mã nguồn:**
```python
import time

query_movies = df_movies_stream.writeStream \
    .format("json") \
    .option("path", "data/movies_static") \
    .option("checkpointLocation", "chk/movies") \
    .outputMode("append") \
    .start()

query_tags = df_tags_stream.writeStream \
    .format("json") \
    .option("path", "data/tags_static") \
    .option("checkpointLocation", "chk/tags") \
    .outputMode("append") \
    .start()

print("Waiting 10 seconds for stream processing...")
time.sleep(10)

query_movies.stop()
query_tags.stop()
print("\n Stream data saved to files.")
```

**Kết quả thực thi:**
```text
Waiting 10 seconds for stream processing...

 Stream data saved to files.
```

### 2.4 Đọc Dữ Liệu Tĩnh từ JSON Files
Tái sử dụng dữ liệu đã ghi làm nguồn batch table cho các task tương quan sau (phục vụ mục đích Static-Stream Join).

**Mã nguồn:**
```python
print("Reading movies from data/movies_static...")
df_movies = spark.read \
    .schema(movie_schema) \
    .json("data/movies_static")
print(f"Movies loaded: {df_movies.count()} rows")

print("Reading tags from data/tags_static...")
df_tags = spark.read \
    .schema(tag_schema) \
    .json("data/tags_static")
print(f"Tags loaded: {df_tags.count()} rows")

print("\n Static data successfully loaded from files.")
```

**Kết quả thực thi:**
```text
Reading movies from data/movies_static...
Movies loaded: 9742 rows
Reading tags from data/tags_static...
Tags loaded: 3683 rows

 Static data successfully loaded from files.
```

### 2.5 Bài tập 2: Hot Genres - Static-Stream Join
**Các bước thực hiện:**
- Kết hợp ratings (Streaming DataFrame) với movies (Batch DataFrame) qua cột `movieId`. Các tác vụ được gọi là luồng phối hợp tĩnh-động (Static-Stream Join).
- Dùng groupby để gom số lượng xếp hạng cho tất cả các loại.
- Xuất kết quả ra màn hình với mode `complete` trong mỗi trigger cách nhau 5 giây.

**Mã nguồn:**
```python
from pyspark.sql.functions import split, explode, desc

# Tách chuỗi genres bằng dấu "|" và tạo nhiều dòng cho mỗi thể loại
df_genres = df_movies.select(
    "movieId", 
    "title", 
    explode(split(col("genres"), "\\|")).alias("genre")
)
print("Created genre mapping")

df_ratings_genres = df_ratings_stream.join(df_genres, on="movieId", how="inner")
print("Joined ratings with genres")

# Gom nhóm theo từng thể loại và sắp xếp giảm dần theo lượt rating
df_genre_agg = df_ratings_genres.groupBy("genre").count().orderBy(desc("count"))
print("Aggregated ratings by genre")

query = (
    df_genre_agg.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)
print("Starting stream output...")

# Chờ 30 giây rồi ép dừng query để giải phóng tài nguyên
query.awaitTermination(timeout=30)
query.stop()
print("Complete")
```

**Kết quả thực thi (Mẫu Batch đầu):**
```text
Created genre mapping
Joined ratings with genres
Aggregated ratings by genre
Starting stream output...
-------------------------------------------
Batch: 0
-------------------------------------------
+-----------+-----+
|genre      |count|
+-----------+-----+
|Drama      |4361 |
|Comedy     |3903 |
|Action     |3020 |
|Thriller   |2646 |
|Adventure  |2416 |
|Romance    |1812 |
|Sci-Fi     |1724 |
|Crime      |1668 |
|Fantasy    |1183 |
|Children   |920  |
|Mystery    |767  |
|Horror     |729  |
|Animation  |698  |
|War        |485  |
|IMAX       |414  |
|Musical    |411  |
|Documentary|122  |
|Western    |119  |
|Film-Noir  |87   |
|(no genres listed)|34   |
+-----------+-----+
only showing top 20 rows

Complete
```

### 2.6 Bài tập 3: Trending Now - Windowed Aggregation with Dense Rank
**Các bước thực hiện:**
- Làm mịn cột `timestamp` của DataStream ratings qua hàm `from_unixtime()`.
- Thiết lập Tumbling Window kéo dài 5 phút trên Event Time và xác nhận hạn mức chướng ngại với thời gian trễ Watermark (5 phút) cho các mốc xếp hạng.
- Sử dụng hàm tạo cột Rank bằng `Window.partitionBy(window)` và `dense_rank()` hỗ trợ để giữ hạng Top 3 trending movie trong mỗi khoảnh khắc thời gian được chiếu.
- Sử dụng `foreachBatch()` API để vận dụng mã thủ tục đếm, trích xuất trên mỗi lô truyền đi.

**Mã nguồn:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc, from_unixtime, window

print("Converting timestamp to TimestampType...")
windowed_counts = df_ratings_stream \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "movieId") \
    .count()
print("Created 5-minute windows and counted by movieId")

print("Joining with movies...")
joined = windowed_counts.join(df_movies, on="movieId", how="inner")
print("Joined with movie titles")

def apply_ranking(batch_df, batch_id):
    window_spec = Window.partitionBy("window").orderBy(desc("count"), "movieId")
    result = batch_df.withColumn("rank", dense_rank().over(window_spec)) \
        .filter("rank <= 3") \
        .select("window", "movieId", "title", "count", "rank")
    
    print(f"\nBatch {batch_id}:")
    result.show(truncate=False)

query = joined.writeStream \
    .outputMode("complete") \
    .foreachBatch(apply_ranking) \
    .trigger(processingTime="5 seconds") \
    .start()

print("Starting trending now stream...")
query.awaitTermination(timeout=30)
print("Complete")
```

**Kết quả thực thi (Mẫu Batch đầu):**
```text
Converting timestamp to TimestampType...
Created 5-minute windows and counted by movieId
Joining with movies...
Joined with movie titles
Starting trending now stream...

Batch 0:
+------------------------------------------+-------+----------------------------------+-----+----+
|window                                    |movieId|title                             |count|rank|
+------------------------------------------+-------+----------------------------------+-----+----+
|{1996-07-10 05:10:00, 1996-07-10 05:15:00}|10     |GoldenEye (1995)                  |1    |1   |
|{1996-07-10 05:10:00, 1996-07-10 05:15:00}|34     |Babe (1995)                       |1    |2   |
|{1996-07-10 05:10:00, 1996-07-10 05:15:00}|47     |Seven (a.k.a. Se7en) (1995)       |1    |3   |
|{1996-08-02 04:10:00, 1996-08-02 04:15:00}|193    |Showgirls (1995)                  |1    |1   |
|{1996-08-19 23:40:00, 1996-08-19 23:45:00}|10     |GoldenEye (1995)                  |1    |1   |
|{1996-08-19 23:40:00, 1996-08-19 23:45:00}|34     |Babe (1995)                       |1    |2   |
|{1996-08-19 23:40:00, 1996-08-19 23:45:00}|48     |Pocahontas (1995)                 |1    |3   |
|{1997-03-30 18:20:00, 1997-03-30 18:25:00}|5      |Father of the Bride Part II (1995)|1    |1   |
|{1997-03-30 18:20:00, 1997-03-30 18:25:00}|6      |Heat (1995)                       |1    |2   |
|{1997-03-30 18:20:00, 1997-03-30 18:25:00}|14     |Nixon (1995)                      |1    |3   |
|{1997-12-04 17:55:00, 1997-12-04 18:00:00}|1466   |Donnie Brasco (1997)              |1    |1   |
|{1998-06-28 16:35:00, 1998-06-28 16:40:00}|58     |Postman, The (Postino, Il) (1994) |1    |1   |
|{1998-06-28 16:35:00, 1998-06-28 16:40:00}|1726   |Postman, The (1997)               |1    |2   |
|{1999-10-13 17:30:00, 1999-10-13 17:35:00}|380    |True Lies (1994)                  |1    |1   |
|{1999-10-13 17:30:00, 1999-10-13 17:35:00}|1968   |Breakfast Club, The (1985)        |1    |2   |
|{1999-10-13 17:30:00, 1999-10-13 17:35:00}|2245   |Working Girl (1988)               |1    |3   |
|{1999-11-03 21:55:00, 1999-11-03 22:00:00}|1079   |Fish Called Wanda, A (1988)       |1    |1   |
|{1999-11-03 21:55:00, 1999-11-03 22:00:00}|1135   |Private Benjamin (1980)           |1    |2   |
|{1999-11-03 21:55:00, 1999-11-03 22:00:00}|1257   |Better Off Dead... (1985)         |1    |3   |
|{1999-11-17 19:25:00, 1999-11-17 19:30:00}|588    |Aladdin (1992)                    |1    |1   |
+------------------------------------------+-------+----------------------------------+-----+----+
only showing top 20 rows

Complete
```

### 2.7 Bài tập 4: Stream-Stream Join - Real-time Tag-Rating Correlation
**Các bước thực hiện:**
- Thực hiện Stream-Stream Join giữa hai luồng (Tags và Ratings). 
- Chỉ định mức kiểm soát dữ liệu muộn (Watermark = 10 phút) trên cả hai streams trước khi hợp nhất để Spark hiểu được kích thước bảo lưu bộ nhớ trung gian (Stateful Management).
- Xuất kết quả với chế độ `append` do đây là logic join bảo toàn bản ghi.

**Mã nguồn:**
```python
print("Preparing tags stream with watermark...")
df_tags_with_watermark = df_tags_stream \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumnRenamed("timestamp", "tagTime") \
    .withWatermark("tagTime", "10 minutes")

print("Preparing ratings stream with watermark...")
df_ratings_with_watermark = df_ratings_stream \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumnRenamed("timestamp", "ratingTime") \
    .withWatermark("ratingTime", "10 minutes")

print("Performing stream-stream join on movieId...")
stream_stream_join = df_tags_with_watermark.join(
    df_ratings_with_watermark,
    on="movieId",
    how="inner"
).select("movieId", "tag", "rating", "ratingTime", "tagTime")

query = (
    stream_stream_join.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)
print("Starting stream-stream join...")
query.awaitTermination(timeout=30)
print("Complete")
```

**Kết quả thực thi (Mẫu Batch đầu):**
```text
Preparing tags stream with watermark...
Preparing ratings stream with watermark...
Performing stream-stream join on movieId...
Starting stream-stream join...
-------------------------------------------
Batch: 0
-------------------------------------------
+-------+-------+------+-------------------+-------------------+
|movieId|tag    |rating|ratingTime         |tagTime            |
+-------+-------+------+-------------------+-------------------+
|1580   |aliens |3.0   |2000-07-31 01:18:45|2006-01-14 09:25:19|
|1088   |dance  |3.0   |2016-02-16 17:41:15|2006-01-27 03:20:56|
|1088   |music  |3.0   |2016-02-16 17:41:15|2006-01-27 03:20:56|
|1580   |aliens |3.5   |2016-02-18 05:33:55|2006-01-14 09:25:19|
|1580   |aliens |4.5   |2014-08-10 03:57:13|2006-01-14 09:25:19|
|1580   |aliens |3.0   |2000-07-04 11:59:18|2006-01-14 09:25:19|
|1580   |aliens |3.0   |2009-02-13 16:08:37|2006-01-14 09:25:19|
|1645   |lawyers|2.5   |2009-05-11 16:12:31|2006-01-16 08:14:55|
|1580   |aliens |2.5   |2017-12-26 04:39:55|2006-01-14 09:25:19|
|1088   |dance  |4.0   |2009-01-03 03:55:36|2006-01-27 03:20:56|
|1088   |music  |4.0   |2009-01-03 03:55:36|2006-01-27 03:20:56|
|1580   |aliens |5.0   |2009-01-03 03:47:34|2006-01-14 09:25:19|
|1645   |lawyers|5.0   |2009-01-03 04:40:02|2006-01-16 08:14:55|
|1580   |aliens |2.0   |2015-09-26 00:11:33|2006-01-14 09:25:19|
|1088   |dance  |4.0   |2006-10-23 06:31:42|2006-01-27 03:20:56|
|1088   |music  |4.0   |2006-10-23 06:31:42|2006-01-27 03:20:56|
|1580   |aliens |4.0   |2017-07-29 03:25:59|2006-01-14 09:25:19|
|1088   |dance  |3.5   |2006-09-18 06:10:14|2006-01-27 03:20:56|
|1088   |music  |3.5   |2006-09-18 06:10:14|2006-01-27 03:20:56|
|1580   |aliens |4.0   |2006-09-18 05:15:46|2006-01-14 09:25:19|
+-------+-------+------+-------------------+-------------------+
only showing top 20 rows

Complete
```

---

## 3. SUBMISSION (CẤU TRÚC THƯ MỤC)
Dưới đây là mô tả các tệp tin và thư mục chính trong bài làm của chúng em:
- **`notebook/LAB2.ipynb`**: Tệp Jupyter Notebook chứa toàn bộ mã nguồn thực thi về quá trình xử lý Spark Streaming.
- **`docker-compose.yml`**: Tệp cấu hình Docker Compose để triển khai cụm Kafka nội bộ. Cụm bao gồm **3 Brokers** hoạt động trong chế độ KRaft (không cần Zookeeper), giúp giả lập môi trường phân tán thực tế.
- **`report.pdf` / `lab2_report.md`**: Bản báo cáo chi tiết, trình bày toàn bộ quá trình thực hiện, định dạng chuẩn hoá dựa trên LAB 1.
- **`requirements.txt`**: Danh sách các thư viện Python cần thiết để chạy notebook.
- **`Makefile`**: Các tập lệnh hỗ trợ khởi động nhanh môi trường Kafka và quản lý các dịch vụ Docker một cách tự động.

---

## 4. HƯỚNG DẪN CHẠY

### Bước 1: Chuẩn bị môi trường
```bash
# Tạo virtual environment
python -m venv bdnotebook

# Kích hoạt environment (WSL)
source bdnotebook/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt
```

### Bước 2: Khởi động Kafka
```bash
# Khởi động Kafka cluster (3 brokers)
docker-compose -f docker-compose.yml up -d
```

### Bước 3: Chạy Jupyter Lab
```bash
jupyter-lab --allow-root
```

### Bước 4: Chạy các cells trong LAB2.ipynb
- Chạy từ trên xuống dưới.
- Mỗi exercise sẽ xuất kết quả theo từng batch.

---

## 5. KẾT LUẬN
Qua bài Lab 2, chúng em đã hoàn thành việc xây dựng một pipeline xử lý dữ liệu Streaming thời gian thực tích hợp giữa Apache Kafka và Apache Spark Streaming. Quá trình thực hành đã giúp củng cố những kỹ năng quan trọng:
- Thiết lập dòng dữ liệu Streaming và khai báo Schema cho quá trình Structured Streaming nhằm kiểm soát dữ liệu thô ngay khi tải từ Message Broker.
- Vận dụng linh hoạt các phép Join như **Static-Stream Join** để ánh xạ metadata, và đặc biệt là **Stream-Stream Join** phục vụ nhu cầu liên kết sự kiện thực ngay khi có chuỗi thay đổi.
- Thao tác thực tiễn tính năng đánh dấu ngưỡng muộn **Windowing & Watermarking** của Spark để gom nhóm theo chu kỳ linh hoạt và kiểm soát tín hiệu trễ.
- Áp dụng các **Window Functions** (điển hình như `dense_rank()`) trong hàm `foreachBatch` của API DataFrame Streaming để đáp ứng truy vấn xếp hạng thứ bậc nâng cao.