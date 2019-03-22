package com.job.task

import com.job.Logger

/**
  * Created by jaesang on 2019-03-22.
  */

case class TaskConf(name: String = "empty",
                    `type`: String = "empty",
                    inputs: Seq[String] = Nil,
                    options: Map[String, String] = Map.empty)


trait Task extends Serializable with Logger {

  val conf: TaskConf

  def mandatoryOptions: Set[String]

  def isValidate: Boolean = mandatoryOptions.subsetOf(conf.options.keySet)

}
