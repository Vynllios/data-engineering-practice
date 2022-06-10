import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
from zipfile import ZipFile


def main():
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()
    cwd=os.getcwd()
    file1=os.path.join(cwd,'data','Divvy_Trips_2019_Q4.zip')
    file2=os.path.join(cwd,'data','Divvy_Trips_2020_Q1.zip')
    # creating spark dataframes for both files.
    # NEED TO FIGURE OUT HOW TO READ CSV FILES WITHIN ZIP FILES USING SPARK WITHOUT EXTRACTING
    csv_path=os.path.join(cwd,'reports')
    try:
        os.mkdir(csv_path)
    except:
        print('path already exists')
    with ZipFile(file1) as myzip:
        filepath=myzip.extract('Divvy_Trips_2019_Q4.csv')
        df=spark.read.options(header='True',inferSchema='True',delimiter=',').csv(filepath)
    df.show(n=10)
    with ZipFile(file2) as myzip:
        filepath=myzip.extract('Divvy_Trips_2020_Q1.csv')
        df2=spark.read.options(header='True',inferSchema='True',delimiter=',').csv(filepath)
    df2.show(n=10)
    df.createTempView('trips')
    q1=spark.sql("SELECT DATE(start_time) as Day,AVG(tripduration) FROM trips GROUP BY Day ORDER BY  Day")
    q1.show()
    question1_path=os.path.join(csv_path,'question1.csv')
    q1.write.option("header", "true").csv(question1_path)
    q2=spark.sql("SELECT COUNT(trip_id),DATE(start_time) as Day FROM trips GROUP BY Day ORDER BY Day")
    q2.show()
    # q2.write.option("header", "true").csv(question2_path)
    os.remove('Divvy_Trips_2019_Q4.csv')
    os.remove('Divvy_Trips_2020_Q1.csv')


if __name__=='__main__':
    main()







