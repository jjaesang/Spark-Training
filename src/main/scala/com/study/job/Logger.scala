package com.study.job

import org.apache.commons.logging.{Log, LogFactory}

/**
  * Created by jaesang on 2019-03-14.
  */
trait Logger {

  @transient lazy val logger: Log = LogFactory.getLog(this.getClass)

}
