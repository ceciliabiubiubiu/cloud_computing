import tensorflow as tf
import tensorflow_hub as hub
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import Word2Vec
import re

#STAGE 1

# Create a SparkSession
spark = SparkSession.builder.appName("MusicDataAnalysis").getOrCreate()

#Load data from S3
music_data = "s3://amazon-reviews-pds/tsv/amazon_reviews_us_Music_v1_00.tsv.gz"
musics = spark.read.csv(music_data,header=True,sep='\t')
musics = musics.drop('marketplace','product_parent','product_title','product_category','helpful_votes','total_votes','vine','verified_purchase','review_headline','review_date').cache()

#Number of items in the dataset = total count of the reviews
musics.count()

#Count unique customer and product id
musics.select("customer_id").distinct().count()
musics.select("product_id").distinct().count()

#Largest number of reviews by a single user
musics.groupby('customer_id').count().sort(f.col("count"), ascending = False).select("count").limit(1).show()

#Top 10 users ranked by reviews
musics.groupby('customer_id').count().sort(f.col("count"), ascending = False).select("customer_id").limit(10).show()

#Median number of review for a user
count_list_user = musics.groupby('customer_id').count().sort(f.col("count"), ascending = False).select("count").selectExpr("count as _count")
count_array_user = [int(row._count) for row in count_list_user.collect()]
median_user = np.median(count_array_user)
print(median_user)

#Largest number of reviews for a single product
musics.groupby('product_id').count().sort(f.col("count"), ascending = False).select("count").limit(1).show()

#Top 10 products ranked by reviews
musics.groupby('product_id').count().sort(f.col("count"), ascending = False).select("product_id").limit(10).show()

#Median number of review for a product
count_list_product = musics.groupby('product_id').count().sort(f.col("count"), ascending = False).select("count").selectExpr("count as _count")
count_array_product = [int(row._count) for row in count_list_product.collect()]
median_product = np.median(count_array_product)
print(median_product)

#STAGE 2

#split review_body
def splitreviews(x):
    try:
        if x !=None:
            x = re.split("[.?!]",x)
        new_x = []
        for i in range(len(x)):
            if x[i] != '':
                new_x.append(x[i])
    except:
        pass
    return new_x
#         return x
filter_udf = f.udf(lambda y: splitreviews(y), ArrayType(StringType()))
music_f = musics.withColumn("review_body", filter_udf("review_body"))
# music_f.show()

#Filter length < 2
givenlength = 2
music_s = music_f.filter(f.size(music_f.review_body) > givenlength).cache()
# music_s.show()
# music_s.select('review_body').show()

#Filter by customer id and product id
c_group = music_s.groupby('customer_id').count().sort(f.col("count"), ascending = False)
c_group.withColumn("count", c_group["count"].cast(IntegerType()))
c_group_filter = c_group.filter(f.col("count")>1).sort(f.col('count'))
music_filter_c = music_s.join(c_group_filter, ["customer_id"], "leftsemi")

p_group = music_filter_c.groupby('product_id').count().sort(f.col("count"), ascending = False)
p_group.withColumn("count", p_group["count"].cast(IntegerType()))
p_group_filter = p_group.filter(f.col("count")>2)
music_filtered = music_filter_c.join(p_group_filter, ["product_id"], "leftsemi")

#Top 10 users ranked by median number of sentences in the reviews they have published
music_filtered_modified = music_filtered.withColumn('num_sentences', f.size("review_body"))
# music_filtered_modified.show(5)
music_filtered_modified_user = music_filtered_modified.groupby('customer_id').agg(f.collect_list("num_sentences").alias("list_num_sentences"))
# music_filtered_modified_user.show(5)

def find_median(values_list):
    try:
        median = np.median(values_list)
        return int(median)
    except Exception:
        return None

median_finder = f.udf(find_median, IntegerType())

music_filtered_modified_user2 = music_filtered_modified_user.withColumn('median', median_finder("list_num_sentences"))
# music_filtered_modified3.sort(f.col("median"), ascending = False).limit(10).show()
music_filtered_modified_user2.sort(f.col("median"), ascending = False).select("customer_id").limit(10).show()

# Top 10 products ranked by median number of sentences in the reviews they have received
music_filtered_modified_product = music_filtered_modified.groupby('product_id').agg(f.collect_list("num_sentences").alias("list_num_sentences"))
# music_filtered_modified_product.show(5)
music_filtered_modified_product2 = music_filtered_modified_product.withColumn('median', median_finder("list_num_sentences"))
# music_filtered_modified_product2.sort(f.col("median"), ascending = False).limit(10).show()
music_filtered_modified_product2.sort(f.col("median"), ascending = False).select("product_id").limit(10).show()

#STAGE 3

#Choose product ID and create two class dataframe
music_product = music_filtered.filter(f.col('product_id') == 'B00006J6VG').drop('product_id','customer_id')
music_positive = music_product.filter(f.col('star_rating') > 3).cache()
music_negative = music_product.filter(f.col('star_rating') < 3).cache()
music_positive = music_positive.drop('star_rating')
music_negative = music_negative.drop('star_rating')
# music_positive.show()

#create rdd for two class
music_p_small = music_positive.limit(music_positive.count())
music_p_small_rdd = music_p_small.rdd.cache()
music_n_small = music_negative.limit(music_negative.count())
music_n_small_rdd = music_n_small.rdd.cache()

#explode review sentences
# music_p_explode = music_positive.select("review_id", f.explode(f.col("review_body")).alias("review_body"))
# music_n_explode = music_negative.select("review_id", f.explode(f.col("review_body")).alias("review_body"))

#encode function
def review_encode(x):

    module_url = "https://tfhub.dev/google/universal-sentence-encoder/2" #@param ["https://tfhub.dev/google/universal-sentence-encoder/2", "https://tfhub.dev/google/universal-sentence-encoder-large/3"]
    embed = hub.Module(module_url)
    review_list = []
    review_number = []
    result_list = []

    temp_x = [t for t in x]

    for i in temp_x:
        rid = i[0]
        r_body = i[1]
#         review_number.append(len(r_body))
        review_list += r_body
        result_list.append(i)

    with tf.Session() as session:
        session.run([tf.global_variables_initializer(), tf.tables_initializer()])
        embeddings = session.run(embed(review_list))


    final_result = []
    n = 0
    for l in result_list:
        rid = l[0]
        r_body = l[1]
        for i in r_body:
#         for vec in embeddings:
            vec_list = [rid, i, embeddings[n]]
            final_result.append(vec_list)
            n+=1

    return final_result
#     return embeddings

#encode for two class
review_embedding_p_s3 = music_p_small_rdd.mapPartitions(review_encode).cache()
# review_embedding_p_s3.count()
review_embedding_n_s3 = music_n_small_rdd.mapPartitions(review_encode).cache()
# review_embedding_n_s3.count()

#create numpy array for all vectors
def return_list(x):
    vec_list = []
    for i in x:
        vec = i[2]
        vec_list.append(vec)
    return vec_list
#positive array
vec_list_p_s3 = review_embedding_p_s3.mapPartitions(return_list).cache()
# vec_list_p_s3.count()
vec_array_p_s3 = np.array(vec_list_p_s3.collect())
# print(vec_array_p_s3.shape)
#negative array
vec_list_n_s3 = review_embedding_n_s3.mapPartitions(return_list).cache()
# vec_list_n_s3.count()
vec_array_n_s3 = np.array(vec_list_n_s3.collect())
# print(vec_array_n_s3.shape)


#caculate positive average distance
def calculate_p_s3_distance(x):
#     temp_x = [t for t in x]
    dis = []
    dis_mean = []
    v1 = x
    for v in vec_array_p_s3:
        dot_value = 1 - float((np.dot(v1, v)) / (np.linalg.norm(v1)*np.linalg.norm(v)))
        dis.append(dot_value)

    a = np.mean(dis)
    dis_mean.append(a)
    return dis_mean

#caculate negative average distance
def calculate_n_s3_distance(x):
#     temp_x = [t for t in x]
    dis = []
    dis_mean = []
    v1 = x
    for v in vec_array_n_s3:
        dot_value = 1 - float((np.dot(v1, v)) / (np.linalg.norm(v1)*np.linalg.norm(v)))
        dis.append(dot_value)

    a = np.mean(dis)
    dis_mean.append(a)
    return dis_mean

#positive average distance
average_distance_p_s3 = vec_list_p_s3.map(calculate_p_s3_distance).cache()
# average_distance_p_s3.take(1)

#negative average distance
average_distance_n_s3 = vec_list_n_s3.map(calculate_n_s3_distance).cache()
# average_distance_n_s3.take(1)


#zip two rdd: (review_id, review_sentence, vector, average_distance)
#positive zip
zip_rdd_p_s3 = review_embedding_p_s3.zip(average_distance_p_s3).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1][0]))
zip_rdd_p_s3.take(5)
#negative zip
zip_rdd_n_s3 = review_embedding_n_s3.zip(average_distance_n_s3).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1][0]))
zip_rdd_n_s3.take(5)


#sort and find center vector
#positive class center
zip_sort_p_s3 = zip_rdd_p_s3.sortBy(lambda x: x[-1])
p_center_list_s3 = zip_sort_p_s3.take(1)
p_center_s3 = (p_center_list_s3[0][0],p_center_list_s3[0][1])
p_center_vec_s3 = p_center_list_s3[0][2]
print(p_center_s3)
#negative class center
zip_sort_n_s3 = zip_rdd_n_s3.sortBy(lambda x: x[-1])
n_center_list_s3 = zip_sort_n_s3.take(1)
n_center_s3 = (n_center_list_s3[0][0],n_center_list_s3[0][1])
n_center_vec_s3 = n_center_list_s3[0][2]
print(n_center_s3)


#find positive nearest 10 vector
def neareast_p_s3_10(x):
    v = x[2]
    rid = x[0]
    dis = []
    dot_value = 1 - float((np.dot(p_center_vec_s3, v)) / (np.linalg.norm(p_center_vec_s3)*np.linalg.norm(v)))

    return (rid, dot_value)

#positive neareast 10
id_dis_p_s3 = review_embedding_p_s3.map(neareast_p_s3_10)
near_10_p_s3 = id_dis_p_s3.sortBy(lambda x: x[1])
near_10_p_s3_list = near_10_p_s3.take(11)
near_10_p_s3_list = near_10_p_s3_list[1::]
for i in near_10_p_s3_list:
    print(i[0])

#find negative nearest 10 vector
def neareast_n_s3_10(x):
    v = x[2]
    rid = x[0]
    dis = []
    dot_value = 1 - float((np.dot(n_center_vec_s3, v)) / (np.linalg.norm(n_center_vec_s3)*np.linalg.norm(v)))

    return (rid, dot_value)

#negative neareast 10
id_dis_n_s3 = review_embedding_n_s3.map(neareast_n_s3_10)
near_10_n_s3 = id_dis_n_s3.sortBy(lambda x: x[1])
near_10_n_s3_list = near_10_n_s3.take(11)
near_10_n_s3_list = near_10_n_s3_list[1::]
for i in near_10_n_s3_list:
    print(i[0])


#STAGE 4

#preprocess function before encode
def review_encode_preprocess(x):
    review_list = []
    result_list = []
    temp_x = [t for t in x]
    for i in temp_x:
        rid = i[0]
        r_body = i[1]
        review_list += r_body
        result_list.append(i)

    final_result = []
    for l in result_list:
        rid = l[0]
        r_body = l[1]
        for i in r_body:
            if(i != " "):
                reformat_list = [rid, i]
                final_result.append(reformat_list)
    return final_result

#Preprocess the rdd from stage 3 for both positive and negative
music_p_small_preprocess = music_p_small_rdd.mapPartitions(review_encode_preprocess).cache()
# music_p_small_preprocess.take(2)
music_n_small_preprocess = music_n_small_rdd.mapPartitions(review_encode_preprocess).cache()
# music_n_small_preprocess.take(2)

#Re-formate the preprocessed rdd to formatted dataframe
music_p_preprocess_reformat = spark.createDataFrame(music_p_small_preprocess)
music_p_preprocess_reformat = music_p_preprocess_reformat.withColumnRenamed('_1', 'review_id').withColumnRenamed('_2', 'review_body')
music_p_preprocess_reformat = music_p_preprocess_reformat.withColumn('review_body', f.ltrim(music_p_preprocess_reformat.review_body))
music_p_preprocess_reformat = music_p_preprocess_reformat.withColumn('review_body', f.rtrim(music_p_preprocess_reformat.review_body))
# music_p_preprocess_reformat.show(5)
music_n_preprocess_reformat = spark.createDataFrame(music_n_small_preprocess)
music_n_preprocess_reformat = music_n_preprocess_reformat.withColumnRenamed('_1', 'review_id').withColumnRenamed('_2', 'review_body')
music_n_preprocess_reformat = music_n_preprocess_reformat.withColumn('review_body', f.ltrim(music_n_preprocess_reformat.review_body))
music_n_preprocess_reformat = music_n_preprocess_reformat.withColumn('review_body', f.rtrim(music_n_preprocess_reformat.review_body))
# music_n_preprocess_reformat.show(5)

#Doing tokenizer with regex to separate every word in review body and filter if empty list
regexTokenizer = RegexTokenizer(gaps = False, pattern = '\w+', inputCol = 'review_body', outputCol = 'review_token')
music_p_preprocess_reformat_token = regexTokenizer.transform(music_p_preprocess_reformat)
music_p_preprocess_reformat_token_filter = music_p_preprocess_reformat_token.filter(f.size('review_token') > 1)
# music_p_preprocess_reformat_token_filter.show(5)
music_n_preprocess_reformat_token = regexTokenizer.transform(music_n_preprocess_reformat)
music_n_preprocess_reformat_token_filter = music_n_preprocess_reformat_token.filter(f.size('review_token') > 1)
# music_n_preprocess_reformat_token_filter.show(5)

#Create an average word vector for each sentence
word2vec = Word2Vec(vectorSize = 512, minCount = 1, inputCol = 'review_token', outputCol = 'vector')
model_p = word2vec.fit(music_p_preprocess_reformat_token_filter)
result_p = model_p.transform(music_p_preprocess_reformat_token_filter)
# result_p.count()
# result_p.show(5)
model_n = word2vec.fit(music_n_preprocess_reformat_token_filter)
result_n = model_n.transform(music_n_preprocess_reformat_token_filter)
# result_n.count()
# result_n.show(5)

#Drop the unwanted column and transfer to rdd type
result_p = result_p.drop('review_token')
result_p_rdd = result_p.rdd
result_n = result_n.drop('review_token')
result_n_rdd = result_n.rdd

#reformat function after encode
def rdd_reformat(x):
    final_result = []
    for l in x:
        rid = l[0]
        r_body = l[1]
        r_vec = l[2].toArray().astype(np.double)
        reformat_list = [rid, r_body, r_vec]
        final_result.append(reformat_list)
    return final_result

#Re-format the rdd to review_id, review_body, vector
result_p_rdd_reformat = result_p_rdd.mapPartitions(rdd_reformat).cache()
# result_p_rdd_reformat.count()
# result_p_rdd_reformat.take(2)
result_n_rdd_reformat = result_n_rdd.mapPartitions(rdd_reformat).cache()
# result_n_rdd_reformat.count()
# result_n_rdd_reformat.take(2)

#create numpy array for all vectors
def np_array_vectors(x):
    vec_list = []
    for i in x:
        vec = i[2]
        vec_list.append(vec)
    return vec_list

#positive array
result_p_vec_list = result_p_rdd_reformat.mapPartitions(np_array_vectors).cache()
result_p_vec_array = np.array(result_p_vec_list.collect())
# print(result_p_vec_array.shape)
# result_p_vec_array[0]
#negative array
result_n_vec_list = result_n_rdd_reformat.mapPartitions(np_array_vectors).cache()
result_n_vec_array = np.array(result_n_vec_list.collect())
# print(result_n_vec_array.shape)
# result_n_vec_array[0]

#caculate positive average distance
def calculate_p_w2v_distance(x):
    dis = []
    dis_mean = []
    v1 = x
    for v in result_p_vec_array:
        dot_value = 1 - float((np.dot(v1, v)) / (np.linalg.norm(v1)*np.linalg.norm(v)))
        dis.append(dot_value)

    a = np.mean(dis)
    dis_mean.append(a)
    return dis_mean

#caculate negative average distance
def calculate_n_w2v_distance(x):
    dis = []
    dis_mean = []
    v1 = x
    for v in result_n_vec_array:
        dot_value = 1 - float((np.dot(v1, v)) / (np.linalg.norm(v1)*np.linalg.norm(v)))
        dis.append(dot_value)

    a = np.mean(dis)
    dis_mean.append(a)
    return dis_mean

#positive average distance (take longer time)
average_distance_p_w2v = result_p_vec_list.map(calculate_p_w2v_distance).cache()
# average_distance_p_w2v.take(5)
#negative average distance (take longer time)
average_distance_n_w2v = result_n_vec_list.map(calculate_n_w2v_distance).cache()
# average_distance_n_w2v.take(5)

#zip two rdd: (review_id, review_sentence, vector, average_distance)
#positive zip
zip_rdd_p_w2v = result_p_rdd_reformat.zip(average_distance_p_w2v).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1][0]))
# zip_rdd_p_w2v.take(5)
#negative zip
zip_rdd_n_w2v = result_n_rdd_reformat.zip(average_distance_n_w2v).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1][0]))
# zip_rdd_n_w2v.take(5)

#sort and find center vector
#positive class center
zip_sort_p_w2v = zip_rdd_p_w2v.sortBy(lambda x: x[-1])
p_center_list_w2v = zip_sort_p_w2v.take(1)
p_center_w2v = (p_center_list_w2v[0][0], p_center_list_w2v[0][1])
p_center_vec_w2v = p_center_list_w2v[0][2]
print(p_center_w2v)
print("-----------------")
#negative class center
zip_sort_n_w2v = zip_rdd_n_w2v.sortBy(lambda x: x[-1])
n_center_list_w2v = zip_sort_n_w2v.take(1)
n_center_w2v = (n_center_list_w2v[0][0], n_center_list_w2v[0][1])
n_center_vec_w2v = n_center_list_w2v[0][2]
print(n_center_w2v)

#find positive nearest 10 vector
def neareast_p_w2v_10(x):
    v = x[2]
    rid = x[0]
    dot_value = 1 - float((np.dot(p_center_vec_w2v, v)) / (np.linalg.norm(p_center_vec_w2v)*np.linalg.norm(v)))
    return (rid, dot_value)

#positive neareast 10
id_dis_p_w2v = result_p_rdd_reformat.map(neareast_p_w2v_10)
near_10_p_w2v = id_dis_p_w2v.sortBy(lambda x: x[1])
near_10_p_w2v_list = near_10_p_w2v.take(11)
near_10_p_w2v_list = near_10_p_w2v_list[1::]
for i in near_10_p_w2v_list:
    print(i[0])

print("-----------------")

#find negative nearest 10 vector
def neareast_n_w2v_10(x):
    v = x[2]
    rid = x[0]
    dot_value = 1 - float((np.dot(n_center_vec_w2v, v)) / (np.linalg.norm(n_center_vec_w2v)*np.linalg.norm(v)))
    return (rid, dot_value)

#negative neareast 10
id_dis_n_w2v = result_n_rdd_reformat.map(neareast_n_w2v_10)
near_10_n_w2v = id_dis_n_w2v.sortBy(lambda x: x[1])
near_10_n_w2v_list = near_10_n_w2v.take(11)
near_10_n_w2v_list = near_10_n_w2v_list[1::]
for i in near_10_n_w2v_list:
    print(i[0])
