package com.job

import com.job.task.process.Process
import com.job.task.sink.Sink
import com.job.task.source.Source

/**
  * Created by jaesang on 2019-03-14.
  */

case class JobDescription(
                           name:String,
                           sources:Seq[Source],
                           processes:Seq[Process],
                           sinks:Seq[Sink]
                         )

object JobDescription extends Logger {

  //

}
