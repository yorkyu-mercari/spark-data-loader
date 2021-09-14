name := "spark-data-loader"

version := "0.1"

scalaVersion := "2.12.14"


val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"                       % sparkVersion    % "provided",
  "org.apache.spark"            %% "spark-sql"                        % sparkVersion    % "provided",
  "org.apache.spark"            %% "spark-mllib"                      % sparkVersion    % "provided",
  "com.google.cloud.spark"      %% "spark-bigquery-with-dependencies" % "0.22.1",
  "com.google.cloud.bigdataoss" %  "gcs-connector"                    % "hadoop3-2.2.2",
  "mysql"                       %  "mysql-connector-java"             % "8.0.26",
  "com.github.scopt"            %% "scopt"                            % "3.7.1",

  "org.scalatest"               %% "scalatest"                        % "3.1.1" % Test,
)

assembly / assemblyShadeRules ++= Seq(
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shaded.fasterxml.jackson.@1").inAll,
  ShadeRule.rename("com.google.**" -> "my.com.google.@1").inAll.inProject,
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".txt" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".java" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".so" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".jnilib" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".dll" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".fmpp" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".tdd" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".ftl" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".json" => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

