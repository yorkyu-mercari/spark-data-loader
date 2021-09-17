## Note
Since this project tested on local spark, and the guava version required by project is 20.0-jre, 
but in spark it was 14.x, so I shaded all `com.google.common` to `my.com.google.common`.

It may not happen on gcp dataproc

## How to debug
### option1
```
val spark: SparkSession = SparkSession.builder
      .master("local[*]") <--------------------- Add this line to where spark session been created,
                                                 And use IDE to execute the main class 
      .appName("Simple Application")
      .getOrCreate()
```

### option2
Add below conf to the spark-submit command on local or remote, and use remote debug
```
...
--conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
--conf "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"
...
```

## Usage (local)
```
./bin/spark-submit \
  --class com.kouzoh.data.loader.tools.EmptyCDCTableCreator \
  --master "local[8]" \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2 \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  /Users/york.yu/IdeaProjects/spark-data-loader/target/scala-2.12/spark-data-loader-assembly-0.3.jar  \
  --projectId [projectId] \
  --datasetName [datasetName] \
  --temporaryGcsBucket [temporaryGcsBucket] \
  --credentialFile [credentialFile] \
  --tableNames [tableNames]
```


## Usage (dataproc)
* the bucket zone should be as same as the zone of dataset in BQ  
```
#1. create a dataproc with specific service account (version = 2.0-debian10, which with spark 3.1.2, hadoop)
#2. ssh into master node by gcloud command
#3. execute command like below
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 1G \
  --executor-memory 2G \
  --executor-cores 2 \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2 \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf "spark.yarn.maxAppAttempts=1" \
  --conf "spark.dynamicAllocation.minExecutors=1" \
  --conf "spark.dynamicAllocation.maxExecutors=5" \
  --conf "spark.executor.memoryOverhead=1G" \
  --class com.kouzoh.data.loader.Main \
  gs://XXXXX/spark-data-loader-assembly-0.3.jar \
  --projectId [projectId] \
  --datasetName [datasetName] \
  --temporaryGcsBucket [bucketName] \
  --suffix __snapshot__ \
  --username [username] \
  --password [password] \
  --cloudSqlInstance [cloudSqlInstance] \
  --dbName [dbName] \
  --tableNames user \
  --splitColumn id \
  --splitCount 100 
```