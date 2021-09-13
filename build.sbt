name := "spark-data-loader"

version := "0.1"

scalaVersion := "2.12.14"


val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"                       % sparkVersion,
  "org.apache.spark"            %% "spark-sql"                        % sparkVersion,
  "org.apache.spark"            %% "spark-mllib"                      % sparkVersion,
  "mysql"                       %  "mysql-connector-java"             % "8.0.26",
  "com.google.cloud.spark"      %% "spark-bigquery-with-dependencies" % "0.22.1",
  "com.google.cloud.bigdataoss" %  "gcs-connector"                    % "hadoop3-2.2.2",
)