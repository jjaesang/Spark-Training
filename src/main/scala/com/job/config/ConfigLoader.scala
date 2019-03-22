package com.job.config

import java.io.File

import com.job.Logger
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by jaesang on 2019-03-22.
  */
class ConfigLoader(configPath: Option[String] = None) extends Logger {

  val config: Config = configPath match {
    case Some(configFilePath) =>
      logger.info(s"load user defined config file : ${configFilePath}")
      loadCustomConfig(configFilePath)

    case _ =>
      logger.info(s"load default config file")
      loadDefaultConfig()
  }

  def getConfiguration: Config = config

  /**
    * 사용자가 지정한 Configuration에 default Configuration (application.conf) Override한 Config 객체 반환
    *
    * @param configFilePath
    * @return Config
    */
  private def loadCustomConfig(configFilePath: String): Config = {

    val customConfigFile = new File(configFilePath)
    if (isNotValidConfigFilePath(customConfigFile))
      throw new Exception(s"'${customConfigFile.getAbsolutePath}' is not exists, or is directory")

    val baseConfig = ConfigFactory.load()
    ConfigFactory
      .parseFile(customConfigFile)
      .withFallback(baseConfig)
  }

  private def loadDefaultConfig(): Config = {
    ConfigFactory.load()
  }

  private def isValidConfigFilePath(configFile: File): Boolean = {
    configFile.exists() &&
      configFile.canRead &&
      !configFile.isDirectory
  }

  private def isNotValidConfigFilePath(configFile: File): Boolean = {
    !isValidConfigFilePath(configFile)
  }

}

/**
  * 테스트
  */
object ConfigLoader extends App {

  val customConfigPath: Some[String] = Some("src/main/resources/custom_application.conf")
  val myConfig = new ConfigLoader(customConfigPath).getConfiguration

  println(s"${myConfig.getString("custom.value")}")
  println(s"${myConfig.getString("phoenix.url")}")
  println(s"${myConfig.getString("phoenix.driver")}")

}