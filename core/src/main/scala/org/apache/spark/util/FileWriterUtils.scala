//package org.apache.spark.util
//
//import java.io.{File, PrintWriter}
//import org.apache.spark.TaskContext
//
//object FileWriterUtils {
//  def writeToFile[T](
//                      taskContext: TaskContext,
//                      dir: String,
//                      label: String,
//                      elements: Iterator[T]
//                    ): Unit = {
//    val taskId = taskContext.taskAttemptId()
//    val partitionId = taskContext.partitionId()
//    val fileName = s"$dir/task-${label}-$taskId-p$partitionId.txt"
//    val file = new File(fileName)
//    val writer = new PrintWriter(file)
//
//    try {
//      elements.foreach(e => writer.println(e.toString))
//    } finally {
//      writer.close()
//    }
//  }
//}
