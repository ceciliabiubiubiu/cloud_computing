
from pyspark import SparkContext
import argparse




def extractVideo(record):
    record = record.strip()
    records = record.split(",")
    video_id = records[0]
    trending_date = records[1]
    category = records[3]
    likes = records[6]
    dislikes = records[7]
    country = records[11]

    return (video_id+"^"+country, category+"^"+trending_date+"^"+likes+"^"+dislikes)




def selectLines(record):
    element1 = record[0]
    ele = element1.split("^")
    id_need = ele[0]
    country = ele[1]


    element2 = record[1]
    info1= element2[0].split("^")
    likes1=int(info1[2])
    dislikes1=int(info1[3])
    category=info1[0]

    info2= element2[1].split("^")
    likes2=int(info2[2])
    dislikes2=int(info2[3])



    result=(dislikes2-dislikes1)-(likes2-likes1)
    return (result, id_need, category, country)


def finalOperation(record):
    result, id_need, category, country=record
    return id_need,result, category, country
   



    

if __name__ == "__main__":
    sc = SparkContext(appName="spark_python")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='')
    parser.add_argument("--output", help="the output path", 
                        default='') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    

    data = sc.textFile(input_path+"AllVideos_short.csv")
    data = data.filter(lambda x: x.split(",")[0]!="video_id")

    data1 = data.map(extractVideo)
    # data2 = data1

    data1 = data1.groupByKey().mapValues(list).filter(lambda x: len(x[1])>1)
    # data2 = data2.groupByKey().mapValues(list)

    data1 = data1.map(selectLines)

    data1 = data1.sortByKey(False,1)

    data1 = data1.map(finalOperation)

    result = data1.take(10)
    result = sc.parallelize(result)


    result.saveAsTextFile("hdfs:///user/hadoop/result888")









