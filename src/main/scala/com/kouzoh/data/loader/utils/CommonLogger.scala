package com.kouzoh.data.loader.utils

import org.slf4j.{Logger, LoggerFactory}

trait CommonLogger {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
}
