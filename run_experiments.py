import os
import sys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import kagglehub

def main():
    # Initialize Spark Session
    spark = (
        SparkSession.builder
        .appName("Lab4_LocalRun")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Spark initialized.")

    # 1. Download data
    print("Downloading MovieLens dataset...")
    path = kagglehub.dataset_download("grouplens/movielens-latest-small")
    print(f"Data downloaded to: {path}")

    # 2. Read datasets
    movies_df = spark.read.csv(path + "/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv(path + "/ratings.csv", header=True, inferSchema=True)
    tags_df = spark.read.csv(path + "/tags.csv", header=True, inferSchema=True)

    movies = movies_df.dropDuplicates(["movieId"]).cache()
    ratings = ratings_df.dropDuplicates(["userId", "movieId"]).cache()
    tags = tags_df.cache()

    print(f"Loaded {movies.count()} movies, {ratings.count()} ratings, and {tags.count()} tags.")

    # ==========================================
    # EXERCISE 1: Binary Classifier
    # ==========================================
    print("\n--- Running Exercise 1: Binary Classifier ---")
    
    # 1. Create label
    ratings_labeled = ratings.withColumn("label", when(col("rating") >= 4.0, 1.0).otherwise(0.0))
    
    # 2. Split into train & test
    train_ratings, test_ratings = ratings_labeled.randomSplit([0.8, 0.2], seed=42)
    train_ratings.cache()
    test_ratings.cache()

    # 3. Compute user/movie aggregates on train set to prevent leakage
    user_aggs = train_ratings.groupBy("userId").agg(
        avg("rating").alias("user_avg_rating"),
        count("rating").alias("user_rating_count")
    ).cache()
    
    movie_aggs = train_ratings.groupBy("movieId").agg(
        avg("rating").alias("movie_avg_rating"),
        count("rating").alias("movie_rating_count")
    ).cache()
    
    global_avg = train_ratings.agg(avg("rating")).first()[0] or 3.5

    # 4. Aggregate tags per user-movie pair
    user_movie_tags = (
        tags.groupBy("userId", "movieId")
        .agg(concat_ws(" ", collect_list("tag")).alias("user_movie_tags"))
        .cache()
    )

    def prepare_features(ratings_df_subset):
        df = (
            ratings_df_subset
            .join(movies, on="movieId", how="inner")
            .join(user_movie_tags, on=["userId", "movieId"], how="left")
            .join(user_aggs, on="userId", how="left")
            .join(movie_aggs, on="movieId", how="left")
        )
        
        # Fill missing values
        df = df.fillna({
            "user_avg_rating": global_avg,
            "user_rating_count": 0,
            "movie_avg_rating": global_avg,
            "movie_rating_count": 0
        })
        
        # Combine text features
        df = df.withColumn(
            "text_raw",
            concat_ws(" ", col("title"), coalesce(col("user_movie_tags"), lit("")))
        )
        
        # Tokenize genres
        df = df.withColumn("genres_tokens", split(col("genres"), "\\|"))
        return df

    train_data = prepare_features(train_ratings).cache()
    test_data = prepare_features(test_ratings).cache()

    # 5. Build Pipeline
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

    tokenizer = Tokenizer(inputCol="text_raw", outputCol="text_words")
    remover = StopWordsRemover(inputCol="text_words", outputCol="text_filtered")
    text_cv = CountVectorizer(inputCol="text_filtered", outputCol="text_tf")
    text_idf = IDF(inputCol="text_tf", outputCol="text_tfidf")
    
    genre_cv = CountVectorizer(inputCol="genres_tokens", outputCol="genres_vector")
    
    assembler = VectorAssembler(
        inputCols=[
            "text_tfidf", 
            "genres_vector", 
            "user_avg_rating", 
            "user_rating_count", 
            "movie_avg_rating", 
            "movie_rating_count"
        ],
        outputCol="features"
    )
    
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    pipeline = Pipeline(stages=[
        tokenizer,
        remover,
        text_cv,
        text_idf,
        genre_cv,
        assembler,
        lr
    ])

    print("Training Logistic Regression pipeline...")
    model = pipeline.fit(train_data)
    print("Training finished.")

    # 6. Evaluate
    predictions = model.transform(test_data).cache()
    
    evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator_auc.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    f1 = evaluator_f1.evaluate(predictions)

    # Confusion matrix
    tp = predictions.filter((col("prediction") == 1.0) & (col("label") == 1.0)).count()
    fp = predictions.filter((col("prediction") == 1.0) & (col("label") == 0.0)).count()
    fn = predictions.filter((col("prediction") == 0.0) & (col("label") == 1.0)).count()
    tn = predictions.filter((col("prediction") == 0.0) & (col("label") == 0.0)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    binary_f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f"AUC: {auc:.4f}")
    print(f"Weighted F1 Score (Multiclass): {f1:.4f}")
    print(f"Binary F1 Score (Class 1 - Rating >= 4): {binary_f1:.4f}")

    print("Confusion Matrix:")
    print(f"               Predicted Negative    Predicted Positive")
    print(f"Actual Negative     {tn:<20}  {fp:<20}")
    print(f"Actual Positive     {fn:<20}  {tp:<20}")

    # 7. Extract Feature Importances
    lr_model = model.stages[-1]
    text_cv_model = model.stages[2]
    genre_cv_model = model.stages[4]
    
    text_vocab = text_cv_model.vocabulary
    genre_vocab = genre_cv_model.vocabulary
    
    feature_names = text_vocab + genre_vocab + [
        "user_avg_rating", 
        "user_rating_count", 
        "movie_avg_rating", 
        "movie_rating_count"
    ]
    
    coefficients = lr_model.coefficients.toArray()
    feature_importance = list(zip(feature_names, coefficients))
    
    # Filter for words/genres (i.e. exclude numeric columns)
    words_genres_importance = [
        (name, coef) for name, coef in feature_importance 
        if name not in ["user_avg_rating", "user_rating_count", "movie_avg_rating", "movie_rating_count"]
    ]
    
    top_positive = sorted(words_genres_importance, key=lambda x: x[1], reverse=True)[:10]
    top_negative = sorted(words_genres_importance, key=lambda x: x[1])[:10]

    print("\nTop 10 Positive Feature Signals:")
    for term, val in top_positive:
        print(f"  {term:<20}: {val:.4f}")

    print("\nTop 10 Negative Feature Signals:")
    for term, val in top_negative:
        print(f"  {term:<20}: {val:.4f}")

    # ==========================================
    # EXERCISE 2: Clustering Model
    # ==========================================
    print("\n--- Running Exercise 2: Clustering ---")
    
    # 1. Aggregate tags per movie
    movie_tags = (
        tags.groupBy("movieId")
        .agg(concat_ws(" ", collect_list("tag")).alias("movie_tags_text"))
    )
    
    movies_with_tags = (
        movies.join(movie_tags, on="movieId", how="left")
        .withColumn("movie_tags_text", coalesce(col("movie_tags_text"), lit("")))
        .withColumn("genres_tokens", split(col("genres"), "\\|"))
        .cache()
    )

    # 2. Build Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import RegexTokenizer, Normalizer

    # Use RegexTokenizer to filter out extra spaces and empty tags (minTokenLength=1 by default)
    tok = RegexTokenizer(inputCol="movie_tags_text", outputCol="words", pattern="\\s+")
    rem = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    cv_t = CountVectorizer(inputCol="filtered_words", outputCol="tag_tf")
    idf_t = IDF(inputCol="tag_tf", outputCol="tag_tfidf")
    
    cv_g = CountVectorizer(inputCol="genres_tokens", outputCol="genres_vector")
    
    # VectorAssembler aggregates raw features
    assembler_c = VectorAssembler(inputCols=["tag_tfidf", "genres_vector"], outputCol="raw_features")
    
    # Add L2 Normalizer to project features to unit length, preventing singleton clusters
    normalizer = Normalizer(inputCol="raw_features", outputCol="features", p=2.0)

    preproc_pipeline = Pipeline(stages=[tok, rem, cv_t, idf_t, cv_g, assembler_c, normalizer])
    preproc_model = preproc_pipeline.fit(movies_with_tags)
    clust_data = preproc_model.transform(movies_with_tags).cache()

    tag_vocab_c = preproc_model.stages[2].vocabulary
    genre_vocab_c = preproc_model.stages[4].vocabulary
    vocab_c = tag_vocab_c + genre_vocab_c
    V_tag = len(tag_vocab_c)

    evaluator_c = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")

    for k in [6, 8, 10, 12]:
        print(f"\nTraining KMeans with K={k}...")
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42)
        km_model = kmeans.fit(clust_data)
        predictions_c = km_model.transform(clust_data).cache()
        
        sil = evaluator_c.evaluate(predictions_c)
        print(f"  K={k} Silhouette Score: {sil:.4f}")
        
        # Get top terms per cluster
        centers = km_model.clusterCenters()
        for i, center in enumerate(centers):
            # Sort dimensions by value descending
            top_dims = np.argsort(center)[::-1][:10]
            cluster_terms = []
            for idx in top_dims:
                if idx < V_tag:
                    cluster_terms.append(f"tag:{tag_vocab_c[idx]}")
                else:
                    cluster_terms.append(f"genre:{genre_vocab_c[idx - V_tag]}")
            
            print(f"  Cluster {i} top terms: {', '.join(cluster_terms)}")
            
            # Sample 10 movies in this cluster
            sample_movies = predictions_c.filter(col("cluster") == i).select("title", "genres").limit(10).collect()
            print(f"  Cluster {i} sample movies:")
            for row in sample_movies:
                print(f"    - {row['title']} ({row['genres']})")
        
        predictions_c.unpersist()

    # ==========================================
    # EXERCISE 3: Recommendation System (ALS)
    # ==========================================
    print("\n--- Running Exercise 3: ALS Recommendation ---")
    
    # 1. Build ALS Recommender
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.evaluation import RegressionEvaluator

    als = ALS(
        maxIter=15,
        regParam=0.1,
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
        seed=42
    )
    
    als_model = als.fit(train_ratings)
    predictions_r = als_model.transform(test_ratings).cache()
    
    evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator_rmse.evaluate(predictions_r)
    print(f"ALS RMSE: {rmse:.4f}")

    # 2. Precision at 10 (averaged across users with >= 1 relevant item in test dataset)
    # Relevant = rating >= 3.0
    user_recs = als_model.recommendForAllUsers(10).cache()
    
    actual_relevant = (
        test_ratings
        .filter(col("rating") >= 3.0)
        .groupBy("userId")
        .agg(collect_set("movieId").alias("actual_movies"))
        .cache()
    )
    
    eval_df = user_recs.join(actual_relevant, on="userId", how="inner")
    
    # Precision calculation using array_intersect
    eval_df = eval_df.withColumn(
        "intersect_size",
        size(array_intersect(col("recommendations.movieId"), col("actual_movies")))
    )
    eval_df = eval_df.withColumn("precision_at_10", col("intersect_size") / 10.0)
    
    avg_precision = eval_df.select(avg("precision_at_10")).first()[0]
    print(f"Precision@10: {avg_precision:.4f}")

    # 3. Print recommendations for 3 random users
    # We can select 3 random users from actual_relevant
    random_users = [row['userId'] for row in actual_relevant.select("userId").distinct().limit(3).collect()]
    print(f"Selected random users for recommendations: {random_users}")
    
    for uid in random_users:
        user_recs_specific = user_recs.filter(col("userId") == uid).select("recommendations").first()
        if user_recs_specific:
            recs = user_recs_specific['recommendations']
            rec_movie_ids = [r['movieId'] for r in recs]
            
            # Fetch movie details including movieId to sort them correctly
            rec_movies_details = (
                movies
                .filter(col("movieId").isin(rec_movie_ids))
                .select("movieId", "title", "genres")
                .collect()
            )
            
            # Map movieId to details row to sort according to Spark recommendations rank
            movie_map = {row['movieId']: row for row in rec_movies_details}
            ordered_recs = [movie_map[mid] for mid in rec_movie_ids if mid in movie_map]
            
            print(f"\nTop 10 Recommendations for User {uid}:")
            for idx, r_movie in enumerate(ordered_recs):
                print(f"  {idx+1:>2}. {r_movie['title']} ({r_movie['genres']})")
                
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
