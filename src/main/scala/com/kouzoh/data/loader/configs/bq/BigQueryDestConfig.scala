package com.kouzoh.data.loader.configs.bq

case class BigQueryDestConfig (projectId: String,
                               datasetName: String,
                               temporaryGcsBucket: String,
                               gcpAccessToken: Option[String],
                               credentialFile: Option[String],
                               partitionKey: Option[String],
                               suffix: Option[String])
