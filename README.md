## Note
Since this project tested on local spark, and the guava version required by project is 20.0-jre, 
but in spark it was 14.x, so I shaded all `com.google` to `my.com.google`.

It may not happen on gcp dataproc


## Usage (local)
```
./bin/spark-submit \
  --class com.kouzoh.data.loader.tools.EmptyCDCTableCreator \
  --master "local[8]" \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2 \
  --conf spark.hadoop.fs.gs.impl=my.com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=my.com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  /Users/york.yu/IdeaProjects/spark-data-loader/target/scala-2.12/spark-data-loader-assembly-0.1.jar  \
  --projectId [projectId] \
  --datasetName [datasetName] \
  --temporaryGcsBucket [temporaryGcsBucket] \
  --credentialFile [credentialFile] \
  --tableNames [tableNames]
```