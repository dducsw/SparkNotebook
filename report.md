# BÁO CÁO THỰC HÀNH BIG DATA (CO3137)
## LAB 1: KAFKA & SPARK TUTORIAL

**Giảng viên hướng dẫn:** Đoàn Ngô Đức Phương  
**Lớp:** L01  
**Ngày thực hiện:** 31/03/2026

### Thông tin nhóm:
- **Lê Đình Đức** - 2310774
- **Nguyễn Văn Công Thành** - 2313133

---

## 1. TỔNG QUAN (OVERVIEW)

### 1.1 Apache Spark
Apache Spark là một công cụ phân tích hợp nhất mạnh mẽ dành cho việc xử lý dữ liệu quy mô lớn. Chúng em tìm hiểu được rằng Spark nổi bật với khả năng xử lý trong bộ nhớ (in-memory), giúp tăng tốc độ đáng kể so với mô hình MapReduce truyền thống. Các đặc điểm chính bao gồm:
- **DAG Scheduler:** Trình lập lịch đồ thị có hướng không chu trình giúp tối ưu hóa thực thi.
- **RDD (Resilient Distributed Datasets):** Cấu trúc dữ liệu nền tảng có khả năng phục hồi lỗi.

### 1.2 Apache Kafka
Apache Kafka là một nền tảng event-streaming phân tán. Chúng em sử dụng Kafka để xử lý các luồng dữ liệu thời gian thực với thông lượng cao. Kafka hoạt động dựa trên mô hình:
- **Append-only log:** Các sự kiện được ghi vào log theo thứ tự.
- **Publish/Subscribe:** Producers gửi dữ liệu vào các Topics, và Consumers đọc dữ liệu từ đó.

---

## 2. TRIỂN KHAI CHI TIẾT (CODE & OUTPUTS)

**Yêu cầu cơ bản của bài tập:**
Bài tập này được thiết kế để giúp chúng em làm quen và thực hành các thao tác quản lý dữ liệu chuyên nghiệp trên Apache Spark và Apache Kafka. Các yêu cầu cụ thể bao gồm:
- **Kết nối Kafka:** Đọc dữ liệu từ 3 brokers tự lưu trữ (self-hosted). Giải quyết lỗi định dạng dữ liệu (`binary/JSON`) phát sinh trong quá trình nhận tin nhắn từ Kafka.
- **Xử lý Top Movies:** Tìm ra top 5 bộ phim (theo ID) có điểm đánh giá trung bình cao nhất, với điều kiện bộ phim đó phải có ít nhất 30 lượt đánh giá.
- **Xử lý Tags:** Xác định 5 nhãn dán (tags) "tệ nhất" – tức là những nhãn dán gắn liền với các bộ phim có điểm trung bình thấp nhất.
- **Phân tích tương quan:** Trả lời câu hỏi liệu các bộ phim mang những tag trên có thực sự có xu hướng bị đánh giá thấp hay không. Với mỗi bộ phim liên quan đến các tag này, cần kiểm tra rating trung bình và số lượng phim cùng bộ tag đó.

### 2.1 Khởi tạo Spark Session
Đầu tiên, chúng em tiến hành khai báo các thư viện cần thiết và cấu hình `SparkSession` để có thể làm việc với Kafka.

Cấu hình Spark để có thể làm việc với Kafka.
Để cấu Spark làm việc với Kafka trong các bài toán xử lý dữ liệu streaming, cần một số denpendencies:
- `spark-sql-kafka` là connector chính cho **Structured Streaming**, cho phép Spark đọc/ghi dữ liệu từ Kafka thông qua API như `readStream`.
- `kafka-clients` là thư viện client cấp thấp dùng để giao tiếp trực tiếp với Kafka broker (được connector sử dụng bên trong).
- `spark-streaming-kafka` phục vụ cho API (connector cho Spark Streaming (DStream API - legacy)) và thường không cần thiết nếu sử dụng Structured Streaming hiện đại.
**Mã nguồn:**
```python
import kagglehub
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

spark = (SparkSession.builder.appName("Lab1_Spark_Kafka")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.kafka:kafka-clients:3.6.0,org.apache.spark:spark-streaming-kafka-0-10_2.13:4.1.1")
    .config("spark.driver.memory", "4g")
    .master("local[*]")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
```

### 2.2 Chuẩn bị Dữ liệu
Chúng em thực hiện tải bộ dữ liệu `movielens-latest-small` từ Kaggle và đọc vào Spark DataFrames.

**Mã nguồn:**
```python
path = kagglehub.dataset_download("grouplens/movielens-latest-small")

df_ratings = spark.read.csv(path + "/ratings.csv", header=True, inferSchema=True)
df_movies = spark.read.csv(path + "/movies.csv", header=True, inferSchema=True)
df_tags = spark.read.csv(path + "/tags.csv", header=True, inferSchema=True)

print("Ratings preview:")
df_ratings.show(3)
print("Movies preview:")
df_movies.show(3)
print("Tags preview:")
df_tags.show(3)
```

**Kết quả thực thi:**
```text
Ratings preview:
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
+------+-------+------+---------+
only showing top 3 rows

Movies preview:
+-------+--------------------+--------------------+
|movieId|               title|              genres|
+-------+--------------------+--------------------+
|      1|    Toy Story (1995)|Adventure|Animati...|
|      2|      Jumanji (1995)|Adventure|Childre...|
|      3|Grumpier Old Men ...|      Comedy|Romance|
+-------+--------------------+--------------------+
only showing top 3 rows

Tags preview:
+------+-------+---------------+----------+
|userId|movieId|            tag| timestamp|
+------+-------+---------------+----------+
|     2|  60756|          funny|1445714994|
|     2|  60756|Highly quotable|1445714996|
|     2|  60756|   will ferrell|1445714992|
+------+-------+---------------+----------+
only showing top 3 rows
```

### 2.3 Thiết lập Kafka Topics
Chúng em định nghĩa 3 brokers và thực hiện xóa các topics cũ (nếu có) trước khi tạo mới các topics `ratings`, `movies`, `tags`.

**Mã nguồn:**
```python
KAFKA_BROKERS = "localhost:9092,localhost:9192,localhost:9292"

# Delete existing topics and create them anew
admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})
admin_client.delete_topics(['ratings', 'movies', 'tags'], operation_timeout=10)
print("Deleted existing topics (if any)")

new_topics = [
    NewTopic(topic="ratings", num_partitions=3, replication_factor=2),
    NewTopic(topic="movies", num_partitions=3, replication_factor=2),
    NewTopic(topic="tags", num_partitions=3, replication_factor=2)
]
fs = admin_client.create_topics(new_topics)
for topic, f in fs.items():
    try:
        f.result() # Wait for topic generation
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}' (Might ignore if it's due to existing topic): {e}")
```

**Kết quả thực thi:**
```text
Deleted existing topics (if any)
Topic 'ratings' created successfully.
Topic 'movies' created successfully.
Topic 'tags' created successfully.
```

### 2.4 Đưa Dữ Liệu Lên Kafka
Dữ liệu từ Spark DataFrames được chúng em chuyển đổi sang định dạng JSON và đẩy vào các topic tương ứng.

**Mã nguồn:**
```python
df_ratings.selectExpr("to_json(struct(*)) AS value") \
    .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", "ratings").save()

df_movies.selectExpr("to_json(struct(*)) AS value") \
    .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", "movies").save()

df_tags.selectExpr("to_json(struct(*)) AS value") \
    .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", "tags").save()

print("Successfully written data to Kafka topics.")
```

**Kết quả thực thi:**
```text
Successfully written data to Kafka topics.
```

### 2.5 Đọc Dữ Liệu Từ Kafka & Xử Lý Lỗi Định Dạng
Để xử lý dữ liệu nhị phân từ Kafka, chúng em định nghĩa Schema và thực hiện ép kiểu (`CAST`) cột `value` sang `STRING`, sau đó dùng hàm `from_json`.

**Giải thích:**
- **Lý do cần StructType (Schema):** Dữ liệu khi đọc từ **Apache Kafka** trong **Apache Spark** thường có dạng nhị phân (`binary`) ở cột `value`, nên không thể xử lý trực tiếp như dữ liệu có cấu trúc. Để khắc phục, ta cần ép kiểu (`cast`) cột `value` sang `STRING`, sau đó sử dụng hàm `from_json` kết hợp với schema đã định nghĩa trước để parse chuỗi JSON này thành các cột có kiểu dữ liệu rõ ràng. Cách này giúp Spark hiểu đúng cấu trúc dữ liệu và tránh các lỗi liên quan đến định dạng khi xử lý streaming.

- **`StructType`** trong Apache Spark là một kiểu dữ liệu thuộc hệ thống schema (cụ thể là `pyspark.sql.types`), dùng để định nghĩa **cấu trúc của một DataFrame** dưới dạng tập hợp các cột có kiểu dữ liệu xác định. Nó bao gồm nhiều `StructField`, trong đó mỗi field mô tả tên cột, kiểu dữ liệu (như `StringType`, `IntegerType`, …) và khả năng null (`nullable`). Trong các bài toán như đọc dữ liệu từ Kafka (JSON dạng string), `StructType` đóng vai trò rất quan trọng khi kết hợp với hàm `from_json` để **parse dữ liệu bán cấu trúc thành dạng có schema rõ ràng**, giúp Spark tối ưu hóa xử lý và tránh lỗi định dạng.
- **Cách Spark đọc Stream từ Kafka:** Chúng em sử dụng API `spark.read` với định dạng `.format("kafka")`. Các tùy chọn (`.option`) quan trọng bao gồm:
    - `kafka.bootstrap.servers`: Danh sách các broker đích (trong bài này là 3 brokers).
    - `subscribe`: Tên topic muốn đăng ký nhận dữ liệu.
    - `startingOffsets`: Vị trí bắt đầu đọc dữ liệu (ví dụ: `earliest`).
    Khi load, Spark trả về một DataFrame chứa các cột hệ thống của Kafka như `key`, `value`, `topic`, `partition`, `offset`, và `timestamp`. Cột `value` chính là nơi chứa dữ liệu thực tế mà chúng em cần parse.

**Mã nguồn:**
```python
# Define Schemas
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

def read_and_parse_kafka(topic, schema):
    """Read from Kafka and parse JSON string"""
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Resolve data format: CAST(value AS STRING)
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")
    
    return df_parsed

df_movies_parsed = read_and_parse_kafka("movies", movie_schema)
df_ratings_parsed = read_and_parse_kafka("ratings", rating_schema)
df_tags_parsed = read_and_parse_kafka("tags", tag_schema)

print("Parsed Movie Data:")
df_movies_parsed.show(5)
print("Parsed Ratings Data:")
df_ratings_parsed.show(5)
print("Parsed Tags Data:")
df_tags_parsed.show(5)
```

**Kết quả thực thi:**
```text
Parsed Movie Data:
+-------+--------------------+--------------------+
|movieId|               title|              genres|
+-------+--------------------+--------------------+
|   1022|   Cinderella (1950)|Animation|Childre...|
|   1023|Winnie the Pooh a...|Animation|Childre...|
|   1024|Three Caballeros,...|Animation|Childre...|
|   1025|Sword in the Ston...|Animation|Childre...|
|   1027|Robin Hood: Princ...|     Adventure|Drama|
+-------+--------------------+--------------------+
only showing top 5 rows

Parsed Ratings Data:
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
+------+-------+------+---------+
only showing top 5 rows

Parsed Tags Data:
+------+-------+-----------+----------+
|userId|movieId|        tag| timestamp|
+------+-------+-----------+----------+
|   424|   2700|     satire|1457844392|
|   424|   2700| south park|1457844407|
|   424|   2700|Trey Parker|1457844412|
|   424|   2959|dark comedy|1457842797|
|   424|   2959| psychology|1457842802|
+------+-------+-----------+----------+
only showing top 5 rows
```

### 2.6 Bài tập 1: Lấy Top 5 Phim Có Xếp Hạng Cao Nhất
Chúng em thực hiện lấy danh sách 5 bộ phim có điểm rating trung bình cao nhất, với điều kiện có số lượng người đánh giá > 30.

**Các bước thực hiện:**
1.  **Nhóm dữ liệu (GroupBy):** Sử dụng hàm `groupBy("movieId")` trên DataFrame ratings đã được parse.
2.  **Tính toán (Aggregation):** Sử dụng `agg` để tính trung bình của cột `rating` (alias: `avg_rating`) và đếm số lượng đánh giá (alias: `rating_count`).
3.  **Lọc dữ liệu (Filtering):** Áp dụng điều kiện `rating_count > 30` để loại bỏ các phim có quá ít đánh giá (tránh sai số thống kê).
4.  **Kết nối bảng (Joining):** Thực hiện `join` với bảng danh mục phim (`df_movies_parsed`) dựa trên `movieId` để lấy thông tin tiêu đề phim.
5.  **Sắp xếp (Sorting):** Sắp xếp kết quả giảm dần theo `avg_rating` và tăng dần theo `movieId`.

**Mã nguồn:**
```python
# Get Top 5 movies in df_ratings
top_5_movies = df_ratings_parsed.groupBy("movieId") \
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    ) \
    .filter(col("rating_count") > 30) \
    .limit(5)

# Join with movies to get tiltle
top_5_movies_with_name = top_5_movies.join(df_movies_parsed, on="movieId", how="inner") \
    .select("movieId", "title", "avg_rating", "rating_count") \
    .orderBy(col("avg_rating").desc(), col("movieId").asc())

print("Top 5 movies (with titles) whose rating count > 30:")
top_5_movies_with_name.show(truncate=False)
```

**Kết quả thực thi:**
```text
Top 5 movies (with titles) whose rating count > 30:
+-------+--------------------------------+-----------------+------------+
|movieId|title                           |avg_rating       |rating_count|
+-------+--------------------------------+-----------------+------------+
|3175   |Galaxy Quest (1999)             |3.58             |75          |
|471    |Hudsucker Proxy, The (1994)     |3.55             |40          |
|1580   |Men in Black (a.k.a. MIB) (1997)|3.487878787878788|165         |
|1645   |The Devil's Advocate (1997)     |3.411764705882353|51          |
|1088   |Dirty Dancing (1987)            |3.369047619047619|42          |
+-------+--------------------------------+-----------------+------------+
```

### 2.7 Bài tập 2: Tìm 5 Tag Tệ Nhất
Chúng em thực hiện kết nối (join) bảng tag và rating qua `movieId` để tìm 5 tags có điểm trung bình thấp nhất.

**Các bước thực hiện:**
1.  **Kết nối dữ liệu:** Thực hiện `inner join` giữa bảng tags (`df_tags_parsed`) và bảng ratings dựa trên cột `movieId`. Việc này gắn nhãn (tag) cho từng đánh giá cụ thể.
2.  **Nhóm theo Tag:** Sử dụng `groupBy("tag")` để tổng hợp tất cả các bộ phim và đánh giá liên quan đến nhãn dán đó.
3.  **Tính điểm trung bình:** Tính điểm rating trung bình (`avg`) cho mỗi nhãn dán.
4.  **Sắp xếp tăng dần:** Sắp xếp theo điểm trung bình từ thấp đến cao để tìm các tag có phản hồi tiêu cực nhất từ người dùng.
5.  **Giới hạn:** Sử dụng `limit(5)` để lấy đúng 5 nhãn dán tệ nhất.
**Mã nguồn:**
```python
df_tags_ratings = df_tags_parsed.join(df_ratings_parsed, on="movieId", how="inner")

worst_5_tags = df_tags_ratings.groupBy("tag") \
    .agg(avg("rating").alias("tag_avg_rating")) \
    .orderBy(col("tag_avg_rating").asc()) \
    .limit(5)

print("5 Worst Tags (lowest average rating):")
worst_5_tags.show()

# Extract tag names to list for the next query
worst_tags_list = [row['tag'] for row in worst_5_tags.collect()]
print(f"List of worst tags: {worst_tags_list}")
```

**Kết quả thực thi:**
```text
5 Worst Tags (lowest average rating):
+--------+------------------+
|     tag|    tag_avg_rating|
+--------+------------------+
|symbolic|               0.5|
|   shark|1.4166666666666667|
|   stage|              1.75|
|   Tokyo|               2.0|
|     SNL|               2.1|
+--------+------------------+

List of worst tags: ['symbolic', 'shark', 'stage', 'Tokyo', 'SNL']
```

### 2.8 Bài tập 3: Phân Tích Sự Tác Động Của Tag
Chúng em thực hiện kiểm tra xem các bộ phim mang các tag trên có thực sự nhận điểm thấp không bằng cách so sánh với trung bình toàn bộ dataset.

**Các bước thực hiện:**
1.  **Lọc Danh sách Phim:** Tạo một DataFrame mới chỉ chứa các bộ phim có gắn ít nhất một trong 5 nhãn dán "tệ nhất" đã tìm thấy ở bài tập trước.
2.  **Thống kê chi tiết từng phim:** Với mỗi bộ phim có tag đó, chúng em tính rating trung bình của chính bộ phim đó để xem phim nào kéo điểm của tag xuống.
3.  **Thống kê số lượng phim:** Đếm số lượng phim (`distinct`) tương ứng với mỗi nhãn dán.
4.  **Tính toán Global Average:** Tính điểm trung bình của TOÀN BỘ tập dữ liệu ratings để làm mốc so sánh (Baseline).
5.  **So sánh và Kết luận:** Tính điểm trung bình của các bộ phim mang tag tệ và so sánh với Baseline để kết luận tính tương quan.
**Mã nguồn:**
```python
# Filter records of movies that contain the worst tags
df_worst_tags_movies = df_tags_parsed.filter(col("tag").isin(worst_tags_list))

# Check average ratings for EACH movie associated with these tags
movie_tag_ratings = df_worst_tags_movies.select("movieId", "tag").distinct() \
    .join(df_ratings_parsed, on="movieId", how="inner") \
    .groupBy("tag", "movieId") \
    .agg(avg("rating").alias("movie_avg_rating")) \
    .orderBy("tag", col("movie_avg_rating").asc())

print("Average ratings for each movie associated with the worst tags:")
movie_tag_ratings.show(15)

# Check how many movies have these tags
movies_per_worst_tag = df_worst_tags_movies.select("movieId", "tag").distinct() \
    .groupBy("tag") \
    .agg(count("movieId").alias("num_movies_with_tag")) \
    .orderBy(col("num_movies_with_tag").desc())

print("How many movies have these tags:")
movies_per_worst_tag.show()

# Overall analysis
overall_avg = df_ratings_parsed.agg(avg("rating")).first()[0] or 0
worst_movies_ratings = df_worst_tags_movies.select("movieId").distinct() \
    .join(df_ratings_parsed, on="movieId", how="inner")
worst_tags_overall_avg = worst_movies_ratings.agg(avg("rating")).first()[0] or 0

print(f"Overall Average Rating in complete Dataset: {overall_avg:.2f}")
print(f"Average Rating of movies with the worst tags: {worst_tags_overall_avg:.2f}")

if worst_tags_overall_avg < overall_avg:
    print("=> Conclusion: Yes, indeed! Movies with these tags tend to receive lower ratings than average.")
else:
    print("=> Conclusion: Not necessarily, deeper analysis is required.")
```

**Kết quả thực thi:**
```text
Average ratings for each movie associated with the worst tags:
+--------+-------+------------------+
|     tag|movieId|  movie_avg_rating|
+--------+-------+------------------+
|     SNL|   2296|               2.1|
|   Tokyo|   6407|               2.0|
|   shark|   1389|1.4166666666666667|
|   stage|   8943|              1.75|
|symbolic|  26717|               0.5|
+--------+-------+------------------+

How many movies have these tags:
+--------+-------------------+
|     tag|num_movies_with_tag|
+--------+-------------------+
|   Tokyo|                  1|
|   shark|                  1|
|     SNL|                  1|
|symbolic|                  1|
|   stage|                  1|
+--------+-------------------+

Overall Average Rating in complete Dataset: 3.50
Average Rating of movies with the worst tags: 1.77
=> Conclusion: Yes, indeed! Movies with these tags tend to receive lower ratings than average.
```

**Kết luận:**

Từ kết quả phân tích trên, chúng em nhận thấy một sự tương quan rất rõ rệt giữa các nhãn dán (tags) và mức độ đánh giá từ người dùng. Cụ thể, các bộ phim mang những tag "tệ nhất" chỉ đạt điểm trung bình vỏn vẹn 1.77, thấp hơn rất nhiều so với mức trung bình chung 3.50 của toàn bộ tập dữ liệu. Điều này cho thấy các từ khóa như *symbolic*, *shark* hay *stage* dường như là những "dấu hiệu" nhận diện các bộ phim có chất lượng chưa tốt hoặc kén người xem. Khoảng cách điểm số lên tới gần 50% là minh chứng thuyết phục rằng các nhãn dán này thực sự phản ánh xu hướng đánh giá tiêu cực từ cộng đồng.


## 3. SUBMISSION (CẤU TRÚC THƯ MỤC)
Dưới đây là mô tả các tệp tin và thư mục chính trong bài làm của chúng em:
- **`notebook/LAB1.ipynb`**: Tệp Jupyter Notebook chứa toàn bộ mã nguồn thực thi, từ khâu tải dữ liệu, đẩy dữ liệu vào Kafka cho đến các bước truy vấn và phân tích dữ liệu trên Spark DataFrame.
- **`docker-compose.yml`**: Tệp cấu hình Docker Compose để triển khai cụm Kafka nội bộ. Cụm bao gồm **3 Brokers** hoạt động trong chế độ KRaft (không cần Zookeeper), giúp giả lập môi trường phân tán thực tế.
- **`report.pdf`**: Bản báo cáo chi tiết (tệp tin này), trình bày toàn bộ quá trình thực hiện, giải thích các khái niệm kỹ thuật và kết quả phân tích bài tập.
- **`requirements.txt`**: Danh sách các thư viện Python cần thiết để chạy notebook (bao gồm `pyspark`, `confluent-kafka`, `kagglehub`).
- **`Makefile` & `kafka.ps1`**: Các tập lệnh (scripts) hỗ trợ khởi động nhanh môi trường Kafka và quản lý các dịch vụ Docker một cách tự động.

---

## 4. KẾT LUẬN
Qua bài Lab 1, chúng em đã hoàn thành việc xây dựng một pipeline xử lý dữ liệu tích hợp giữa Apache Kafka và Apache Spark. Quá trình thực hành đã giúp củng cố những kỹ năng quan trọng thao tác với hệ thống phân tán:
- Thiết lập và tương tác với cụm Kafka đa Broker, đảm bảo tính liên tục của luồng dữ liệu.
- Vận dụng Structured Streaming và hệ thống Schema (`StructType`) để tiền xử lý và chuyển đổi dữ liệu thô từ dạng JSON sang định dạng có cấu trúc.
- Thực hiện hiệu quả các phép toán biến đổi cốt lõi trên Spark DataFrame (như Join, Aggregation, GroupBy).
- Triển khai thành công các truy vấn để rút ra những nhận định phân tích có ý nghĩa thực tiễn từ bộ dữ liệu MovieLens.
