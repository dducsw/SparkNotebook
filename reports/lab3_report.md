# BÁO CÁO THỰC HÀNH BIG DATA (CO3137)
## LAB 3: SPARK GRAPH-X

**Giảng viên hướng dẫn:** Đoàn Ngô Đức Phương  
**Lớp:** L01  
**Ngày thực hiện:** 28/04/2026

### Thông tin nhóm:
- **Lê Đình Đức** - 2310774
- **Nguyễn Văn Công Thành** - 2313133

---

## 1. TỔNG QUAN (OVERVIEW)

### 1.1 Apache Spark GraphX (GraphFrames)
Spark GraphFrames là một gói thư viện dành cho Apache Spark nhằm cung cấp API dựa trên DataFrame để làm việc với đồ thị (graphs). Nó thừa hưởng các tính năng của Spark SQL và DataFrames, hỗ trợ phân tích và truy vấn dữ liệu dạng đồ thị hiệu quả.
Các đặc điểm chính bao gồm:
- **Vertices và Edges:** Đồ thị được biểu diễn bởi hai DataFrames: một cho tập hợp các đỉnh (vertices) và một cho các cạnh (edges) nối giữa các đỉnh.
- **Motif finding:** Cho phép tìm kiếm các cấu trúc mẫu (motifs) lặp lại trong đồ thị sử dụng một ngôn ngữ truy vấn riêng biệt, hữu ích để phát hiện các mối quan hệ đặc thù.
- **Graph Algorithms:** Hỗ trợ nhiều thuật toán xử lý đồ thị sẵn có như PageRank, Connected Components, Breadth-First Search (BFS), Triangle Counting, v.v.

### 1.2 Các thành phần trong Lab 3
Trong bài Lab 3, đồ thị được xây dựng trên bộ dữ liệu MovieLens:
- **Vertices:** Bao gồm hai loại đỉnh là người dùng (`user`) và bộ phim (`movie`).
- **Edges:** Đại diện cho các đánh giá (ratings) mà người dùng dành cho bộ phim, với trọng số (`weight`) là điểm đánh giá của người dùng.

Các khái niệm chính được áp dụng:
- **In-Degree và Weighted In-Degree:** `inDegree` biểu thị số lượng cạnh đi vào một đỉnh (tương đương với số lượng người dùng đánh giá một bộ phim). `weightedInDegree` là tổng trọng số của các cạnh đi vào (tổng điểm đánh giá). Hai chỉ số này dùng để đo lường độ phổ biến (popularity bias).
- **PageRank:** Thuật toán phân tích liên kết dùng để đánh giá tầm quan trọng của các đỉnh trong đồ thị. Các bộ phim được nhiều người dùng (đặc biệt là những người dùng có ảnh hưởng) đánh giá sẽ có điểm PageRank cao.
- **Motifs cho Polarization (Sự phân cực):** Tìm kiếm các cặp người dùng có sự bất đồng lớn (chênh lệch điểm đánh giá $\ge 3.0$) khi cùng đánh giá một bộ phim, giúp phân tích sự phân cực của bộ phim đó.

---

## 2. TRIỂN KHAI CHI TIẾT (CODE & OUTPUTS)

**Yêu cầu cơ bản của bài tập:**
Bài tập được thiết kế để áp dụng thư viện GraphFrames vào việc phân tích đồ thị người dùng - bộ phim. Các yêu cầu bao gồm:
- **Exercise 0 (Prepare movie data):** Đọc dữ liệu `movies`, `ratings`, `tags` từ Kafka. Khởi tạo đồ thị với Vertices (người dùng, bộ phim) và Edges (đánh giá).
- **Exercise 1 (Check for popularity bias):** Tính `inDegree` và `weightedInDegree` cho từng bộ phim để đánh giá thiên kiến phổ biến.
- **Exercise 2 (Get the 20 most relevant movies using PageRank):** Chạy thuật toán toàn cục PageRank trên đồ thị và liệt kê Top 20 bộ phim nổi bật.
- **Exercise 3 (Motifs for polarization):** Tìm kiếm cấu trúc mẫu (motifs) của các bộ phim gây nhiều tranh cãi (bất đồng ý kiến sâu sắc giữa các người dùng).

### 2.1 Khởi tạo Spark Session và Dữ liệu Kafka (Exercise 0)
Chúng em cấu hình Spark với gói `graphframes-spark` và đọc dữ liệu MovieLens từ Kaggle, sau đó tạo các topic trên Kafka và đưa dữ liệu vào.
Kế tiếp, tiến hành định nghĩa Schema cho các DataFrame và đọc streaming data từ Kafka.

**Mã nguồn đọc từ Kafka:**
```python
def read_kafka_topic(topic_name, schema):
    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    return (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

movies = read_kafka_topic(TOPIC_MOVIES, movie_schema).dropDuplicates(["movieId"])
ratings = read_kafka_topic(TOPIC_RATINGS, rating_schema).dropDuplicates(["userId", "movieId"])
tags = read_kafka_topic(TOPIC_TAGS, tag_schema)

print(f"Movies: {movies.count()} rows")
print(f"Ratings: {ratings.count()} rows")
print(f"Tags: {tags.count()} rows")
```

**Kết quả thực thi:**
```text
Movies: 9742 rows
Ratings: 100836 rows
Tags: 3683 rows
```

### 2.2 Xây dựng Đồ thị (GraphFrames)
Tạo `vertices` từ DataFrame `movies` và `ratings` (lấy `userId`). Tạo `edges` từ DataFrame `ratings` với `src` là người dùng và `dst` là bộ phim.

**Mã nguồn:**
```python
import numpy as np
from graphframes import GraphFrame

user_vertices = (
    ratings
    .select(col("userId"))
    .distinct()
    .withColumn("id", concat(lit("u_"), col("userId")))
    .withColumn("type", lit("user"))
    .select("id", "type", "userId")
)

movie_vertices = (
    movies
    .withColumn("id", concat(lit("m_"), col("movieId")))
    .withColumn("type", lit("movie"))
    .select("id", "type", "movieId", "title", "genres")
)

vertices = user_vertices.unionByName(movie_vertices, allowMissingColumns=True)

edges = (
    ratings
    .withColumn("src", concat(lit("u_"), col("userId")))
    .withColumn("dst", concat(lit("m_"), col("movieId")))
    .select("src", "dst", "userId", "movieId", col("rating").alias("weight"), "timestamp")
)

g = GraphFrame(vertices, edges)

print(f"Vertices: {g.vertices.count()} rows")
print(f"Edges: {g.edges.count()} rows")
```

**Kết quả thực thi:**
```text
Vertices: 10352 rows
Edges: 100836 rows
```

### 2.3 Bài tập 1: Check for popularity bias

**Mục tiêu / Yêu cầu:**
Popularity bias (thiên kiến phổ biến) xảy ra khi một mục phổ biến được đánh giá cao một cách không cân xứng so với chất lượng thực sự của nó. Lấy ví dụ như các bài hát của một ca sĩ nổi tiếng thu hút hàng triệu lượt nghe/xem bất chấp chất lượng, nguyên nhân chủ yếu đến từ việc "cày view" của lực lượng fan trung thành (Hãy nghĩ đến những bài hát của J97 thu hút hàng triệu lượt xem dù chất lượng đều tệ; điều này là do hiện tượng "cày view" từ Đom đóm - những người hâm mộ trung thành của anh Phương Tuấn).
- **Yêu cầu 1:** Tính toán in-degree của các bộ phim (số lượng người đánh giá riêng biệt - distinct raters) và weighted in-degree (tổng trọng số của các đánh giá).
- **Yêu cầu 2 (Output):** Xuất ra Top 20 bộ phim theo in-degree và weighted in-degree. Đưa ra 3 nhận xét ngắn gọn (insights) về kết quả thu được.

**Các bước thực hiện:**
- **Bước 1: Tính toán các độ đo Bậc vào (In-Degree) cho đồ thị:** Chúng em tiến hành nhóm (`groupBy`) toàn bộ các cạnh đánh giá (`ratings`) theo đỉnh là các bộ phim (`movieId`). 
- **Bước 2: Sử dụng các hàm tổng hợp (`agg`):**
  - Đếm số lượng người dùng duy nhất (`countDistinct("userId")`) để tạo ra chỉ số **inDegree**. Đây là đại lượng thể hiện số lượng người tương tác (tức mức độ phổ biến hay số lượng khán giả) của một bộ phim.
  - Tính tổng các điểm đánh giá (`spark_sum("rating")`) để tính toán **weightedInDegree**. Chỉ số này không chỉ phản ánh mức độ quan tâm mà còn tích lũy tổng điểm số do người dùng chấm, cho thấy sự kết hợp giữa số lượng khán giả và sự ưu ái họ dành cho phim.
- **Bước 3: Lấy thông tin bổ sung:** Kết nối (Join) kết quả thu được với DataFrame `movies` để ánh xạ thêm thông tin định danh như tựa đề (`title`) và thể loại (`genres`).
- **Bước 4: Trích xuất và phân tích:** Sắp xếp giảm dần (`orderBy(desc(...))`) và lấy Top 20 (`limit(20)`) bộ phim đứng đầu theo từng chỉ số (inDegree và weightedInDegree) để chuẩn bị cho việc so sánh và rút ra nhận xét.

**Mã nguồn:**
```python
from pyspark.sql.functions import countDistinct, desc
from pyspark.sql.functions import sum as spark_sum

movie_popularity = (
    ratings
    .groupBy("movieId")
    .agg(
        countDistinct("userId").alias("inDegree"),
        spark_sum("rating").alias("weightedInDegree")
    )
    .join(movies, on="movieId", how="inner")
    .select("movieId", "title", "genres", "inDegree", "weightedInDegree")
)

top20_by_indegree = movie_popularity.orderBy(desc("inDegree"), desc("weightedInDegree")).limit(20)
top20_by_weighted_indegree = movie_popularity.orderBy(desc("weightedInDegree"), desc("inDegree")).limit(20)

print("Top 20 movies by in-degree:")
top20_by_indegree.show(20, truncate=False)

print("Top 20 movies by weighted in-degree:")
top20_by_weighted_indegree.show(20, truncate=False)
```

**Kết quả thực thi (Top 5 mẫu):**
```text
Top 20 movies by in-degree:
+-------+--------------------------------+--------+----------------+
|movieId|title                           |inDegree|weightedInDegree|
+-------+--------------------------------+--------+----------------+
|356    |Forrest Gump (1994)             |329     |1370.0          |
|318    |Shawshank Redemption, The (1994)|317     |1404.0          |
|296    |Pulp Fiction (1994)             |307     |1288.5          |
|593    |Silence of the Lambs, The (1991)|279     |1161.0          |
|2571   |Matrix, The (1999)              |278     |1165.5          |
...
```

**Insights:**
1. Các bộ phim xuất hiện gần đầu của cả hai bảng xếp hạng cho thấy thiên kiến phổ biến (popularity bias), vì lượng khán giả lớn cũng tạo ra tổng điểm đánh giá rất lớn.
2. Sự khác biệt giữa hai bảng xếp hạng tiết lộ những bộ phim mà sự phổ biến không đi kèm với tổng điểm cao tương ứng, hoặc lượng khán giả ít nhưng lại chấm điểm rất cao.
3. `weightedInDegree` không nên được xem như chất lượng thuần túy vì nó tạo ưu thế cho số lượng người đánh giá và khuếch đại các phim vốn đã nổi tiếng.

### 2.4 Bài tập 2: Get the 20 most relevant movies using PageRank

**Mục tiêu / Yêu cầu:**
- **Yêu cầu 1:** Chạy thuật toán toàn cục PageRank trên đồ thị liên kết user -> movie.
- **Yêu cầu 2 (Output):** Xuất ra Top 20 bộ phim dựa trên điểm số PageRank toàn cục, đi kèm với thể loại (genres) và điểm số PageRank của chúng.

**Các bước thực hiện:**
- **Bước 1: Thực thi thuật toán PageRank:** Gọi hàm `g.pageRank()` trên toàn bộ đồ thị mạng lưới người dùng - bộ phim. Hàm sử dụng 2 tham số quan trọng:
  - `resetProbability=0.15`: Là xác suất nhảy ngẫu nhiên (damping factor) có giá trị chuẩn là 0.15 (tương đương với xác suất tiếp tục đi theo các liên kết là 0.85). Giá trị này giúp xử lý trường hợp các đỉnh dạng "hố đen" (không có liên kết ra ngoài) và đảm bảo thuật toán sẽ hội tụ. Trong bối cảnh recommender, tham số này cho phép việc "lang thang ngẫu nhiên" giữa các người dùng và phim để tránh bế tắc mạng lưới.
  - `maxIter=10`: Số vòng lặp tối đa của thuật toán. Thông thường với tập dữ liệu kích thước nhỏ hoặc vừa, 10 vòng lặp là đủ để điểm số PageRank hội tụ một cách tương đối, mang lại kết quả xếp hạng chính xác và tiết kiệm tài nguyên tính toán (thời gian lặp).
- **Bước 2: Lọc các đỉnh kết quả:** Kết quả trả về của thuật toán chứa cả `user` và `movie`. Ta dùng filter `col("type") == "movie"` để chỉ giữ lại các điểm số của các bộ phim.
- **Bước 3: Sắp xếp và trình bày:** Thực hiện sắp xếp giảm dần theo thuộc tính `pagerank` (`pageRankScore`) và in ra màn hình 20 bộ phim có điểm số cao nhất. Bộ phim có điểm số PageRank càng cao thì nó càng được đánh giá bởi những người dùng có xu hướng đánh giá nhiều bộ phim nổi bật khác (người dùng uy tín).

**Mã nguồn:**
```python
pagerank_result = g.pageRank(resetProbability=0.15, maxIter=10)

top20_pagerank_movies = (
    pagerank_result.vertices
    .filter(col("type") == "movie")
    .select("movieId", "title", "genres", col("pagerank").alias("pageRankScore"))
    .orderBy(desc("pageRankScore"))
    .limit(20)
)

top20_pagerank_movies.show(20, truncate=False)
```

**Kết quả thực thi (Top 5 mẫu):**
```text
+-------+--------------------------------+------------------+
|movieId|title                           |pageRankScore     |
+-------+--------------------------------+------------------+
|318    |Shawshank Redemption, The (1994)|4.544090131790101 |
|356    |Forrest Gump (1994)             |4.227459071298885 |
|296    |Pulp Fiction (1994)             |4.022161161548665 |
|593    |Silence of the Lambs, The (1991)|3.793749529100372 |
|2571   |Matrix, The (1999)              |3.5484553443912965|
...
```

### 2.5 Bài tập 3: Motifs for polarization (Bonus)

**Mục tiêu / Yêu cầu:**
- **Yêu cầu 1:** Tìm kiếm các bộ phim gây ra sự phân cực (tạo ra sự bất đồng/tranh cãi). Xếp hạng chúng dựa trên số lượng cặp phân cực (polarized pair count). Một cặp phân cực nghĩa là có hai người dùng đánh giá cùng một bộ phim với độ chênh lệch tuyệt đối ít nhất là 3.0 điểm.
- **Yêu cầu 2 (Output):** Trả về một DataFrame chứa Top 10 bộ phim gây phân cực nhất, bao gồm `movieId`, `title`, và số lượng cặp phân cực (`number of polarized pairs`). Đưa ra 3 nhận xét ngắn gọn (insights) về kết quả thu được.

**Các bước thực hiện:**
- **Bước 1: Tìm kiếm theo cấu trúc Mẫu (Motif Finding):** Sử dụng hàm `g.find()` với cú pháp mẫu `"(u1)-[r1]->(m); (u2)-[r2]->(m)"`. Cấu trúc này dùng để tìm kiếm mô hình nơi 2 đỉnh độc lập (`u1`, `u2`) cùng có cạnh nối (`r1`, `r2`) trỏ đến chung một đỉnh đích (`m`).
- **Bước 2: Loại bỏ nhiễu và tinh chỉnh dữ liệu:**
  - Lọc loại đỉnh: Ràng buộc chắc chắn `u1`, `u2` phải thuộc loại `user` và `m` là `movie`.
  - Loại bỏ các cặp lặp trùng lặp (ví dụ: `u1, u2` và `u2, u1`): Đặt điều kiện định danh `col("u1.id") < col("u2.id")` để chỉ giữ lại 1 cặp kết hợp hợp lệ duy nhất.
- **Bước 3: Điều kiện xác định sự phân cực:** Xác định sự phân cực (trái chiều) bằng cách áp dụng bộ lọc hiệu số tuyệt đối của điểm đánh giá: `spark_abs(col("r1.weight") - col("r2.weight")) >= 3.0`. Điểm số lệch lớn hơn hoặc bằng 3.0 (trên thang điểm 5) biểu thị có sự mâu thuẫn rất mạnh (ví dụ 1 người chấm 5 sao, người kia chấm 1 hoặc 2 sao).
- **Bước 4: Thống kê số lượng cặp phân cực:** Tiến hành gom nhóm kết quả trả về theo `movieId` (và lấy kèm `title`), dùng phép `count()` để đếm xem mỗi bộ phim có bao nhiêu cặp người dùng mâu thuẫn mạnh mẽ. Cuối cùng, sắp xếp và in ra 10 bộ phim gây phân cực mạnh nhất.

**Mã nguồn:**
```python
polarized_motifs = (
    g.find("(u1)-[r1]->(m); (u2)-[r2]->(m)")
    .filter(col("u1.type") == "user")
    .filter(col("u2.type") == "user")
    .filter(col("m.type") == "movie")
    .filter(col("u1.id") < col("u2.id"))
    .filter(spark_abs(col("r1.weight") - col("r2.weight")) >= 3.0)
)

top10_polarizing_movies = (
    polarized_motifs
    .groupBy(col("m.movieId").alias("movieId"), col("m.title").alias("title"))
    .count()
    .withColumnRenamed("count", "polarized_pairs")
    .orderBy(desc("polarized_pairs"))
    .limit(10)
)

top10_polarizing_movies.show(10, truncate=False)
```

**Kết quả thực thi:**
```text
+-------+---------------------------------------------------------+---------------+
|movieId|title                                                    |polarized_pairs|
+-------+---------------------------------------------------------+---------------+
|296    |Pulp Fiction (1994)                                      |3083           |
|2571   |Matrix, The (1999)                                       |2633           |
|527    |Schindler's List (1993)                                  |1747           |
|356    |Forrest Gump (1994)                                      |1569           |
|110    |Braveheart (1995)                                        |1555           |
|2858   |American Beauty (1999)                                   |1499           |
|260    |Star Wars: Episode IV - A New Hope (1977)                |1485           |
|593    |Silence of the Lambs, The (1991)                         |1402           |
|4993   |Lord of the Rings: The Fellowship of the Ring, The (2001)|1178           |
|780    |Independence Day (a.k.a. ID4) (1996)                     |1166           |
+-------+---------------------------------------------------------+---------------+
```

**Insights:**
1. Các bộ phim phân cực cao (highly polarizing) thường có đủ lượng người xem để tạo ra nhiều cặp người dùng đối lập, vì vậy sự tranh cãi thường xuất hiện đi kèm với sự phổ biến.
2. Một lượng lớn cặp đánh giá phân cực cho thấy rằng điểm trung bình (average rating) đã che lấp đi sự bất đồng và cần được phân tích kết hợp với độ phân tán điểm số.
3. Các hệ thống khuyến nghị (Recommender systems) nên xử lý các bộ phim này một cách cẩn thận vì cùng một tác phẩm nhưng có thể được một nhóm người dùng cực kỳ yêu thích trong khi nhóm khác lại vô cùng ghét.

---

## 3. SUBMISSION (CẤU TRÚC THƯ MỤC)
Dưới đây là mô tả các tệp tin và thư mục chính trong bài làm của chúng em:
- **`notebook/LAB3.ipynb`**: Tệp Jupyter Notebook chứa toàn bộ mã nguồn thực thi về quá trình xử lý GraphFrames.
- **`docker-compose.yml`**: Tệp cấu hình Docker Compose để triển khai cụm Kafka nội bộ. 
- **`reports/lab3_report.md`**: Bản báo cáo chi tiết, trình bày toàn bộ quá trình thực hiện thuật toán đồ thị.
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
# Khởi động Kafka cluster
docker-compose -f docker-compose.yml up -d
```

### Bước 3: Chạy Jupyter Lab
```bash
jupyter-lab --allow-root
```

### Bước 4: Chạy các cells trong LAB3.ipynb
- Đảm bảo package `graphframes` đã được load trong cấu hình khởi tạo `SparkSession`.
- Chạy từ trên xuống dưới các ô notebook. Các thuật toán như PageRank hoặc Motifs có thể mất một vài phút để hoàn thành do đồ thị lớn.

---

## 5. KẾT LUẬN
Qua bài Lab 3, chúng em đã làm quen và thực hành phân tích dữ liệu dạng đồ thị (Graph Analytics) sử dụng thư viện GraphFrames trên nền tảng Apache Spark. Các kết quả đạt được:
- Xây dựng thành công đồ thị phân tập (Bipartite Graph) bao gồm các đỉnh (Người dùng, Bộ phim) và các cạnh biểu diễn đánh giá.
- Đo lường và đánh giá thiên kiến phổ biến (Popularity bias) thông qua tính toán Bậc vào (`inDegree`) và Bậc vào có trọng số (`weightedInDegree`).
- Vận dụng linh hoạt thuật toán phân tích liên kết toàn cục **PageRank** để đánh giá tầm quan trọng của các bộ phim trong mạng lưới người dùng.
- Hiểu và triển khai ngôn ngữ truy vấn mẫu **Motifs** để phát hiện các mối quan hệ đặc thù, cụ thể là phân tích mức độ phân cực (Polarization) của những tác phẩm gây tranh cãi.
- Rút ra những insights có giá trị về bản chất đánh giá của người dùng và các bài học lưu ý khi thiết kế hệ thống khuyến nghị (Recommender Systems).
