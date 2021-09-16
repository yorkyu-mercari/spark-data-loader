package com.kouzoh.data.loader.utils

import com.google.api.client.http.HttpRequestInitializer
import com.google.api.services.sqladmin.SQLAdminScopes
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.sql.CredentialFactory
import com.kouzoh.data.loader.utils.ServiceAccountCredentialFactory.CREDENTIAL_FILE_PATH

import java.io.FileInputStream
import collection.JavaConverters._

class ServiceAccountCredentialFactory extends CredentialFactory {
  override def create(): HttpRequestInitializer = {
    val jsonPath: String = System.getProperty(CREDENTIAL_FILE_PATH)
    var credentials: GoogleCredentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))

    if (credentials.createScopedRequired()) {
      credentials = credentials.createScoped(
        Seq(
          SQLAdminScopes.SQLSERVICE_ADMIN,
          SQLAdminScopes.CLOUD_PLATFORM
        ).asJava
      )
    }
    new HttpCredentialsAdapter(credentials)
  }
}

object ServiceAccountCredentialFactory {
  val CREDENTIAL_FILE_PATH = "MY_CREDENTIAL_FILE_PATH"
}
