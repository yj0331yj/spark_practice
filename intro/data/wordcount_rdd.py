from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # SparkSession 객체 생성
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("wordCount RDD ver") \
        .getOrCreate()

    # RDD 자료구조 사용을 위해 SparkSession으로부터 SparkContext를 가져와야 함
    sc: SparkContext = ss.sparkContext


    # load data
    text_file: RDD[str] = sc.textFile("file:////Users/jeong-yoonjin/yeardream/Realtime/spark_practice/spark_practice/data/words.txt")

    # transformations
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda count1, count2: count1 + count2) # 같은 key들의 count을 누적 연산 시킴

    # action : 액션 트리거
    output = counts.collect()

    # show result
    for (word, count) in output:
        print("%s: %i" % (word, count))