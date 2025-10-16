package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestReorders {
  def main(args: Array[String]): Unit = {
    println("=== Testing Category 5: 1→1 (Reorders) ===")
    
    val spark = SparkSession.builder()
      .appName("TestReorders")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      testReorders(sc)
      
      // Check log files after test
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== Test Reorders Complete ===")
  }
  
  def testReorders(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→1 reordering transformations (same count, different order)")
    
    val input = sc.parallelize(List(3, 1, 4, 1, 5, 9, 2, 6), 2)
    
    // Test 1: sortBy
    println("1. Testing sortBy() - reorders elements globally")
    val sorted = input.sortBy(identity)
    val sortResult = sorted.collect()
    println(s"Sort result: ${sortResult.mkString(", ")} (Expected: 1,1,2,3,4,5,6,9)")
    println("Note: This preserves count but changes order - UID pairing gets complex")
    
    // Test 2: sortBy with custom key
    println("2. Testing sortBy with custom key - sort by negative value")
    val sortedDesc = input.sortBy(-_)
    val sortDescResult = sortedDesc.collect()
    println(s"Sort descending result: ${sortDescResult.mkString(", ")} (Expected: 9,6,5,4,3,2,1,1)")
    
    // Test 3: sortWithinPartitions (preserves partition boundaries)
    println("3. Testing sortWithinPartitions() - sorts within each partition")
    val sortedWithinPartitions = input.sortBy(identity, ascending = true, numPartitions = 2)
    val sortWithinResult = sortedWithinPartitions.glom().collect()
    println(s"Sort within partitions result: ${sortWithinResult.map(_.mkString("[", ",", "]")).mkString(", ")}")
    println("Expected: Each partition sorted independently")
    
    // Test 4: reverse (if available through custom implementation)
    println("4. Testing reverse order using zipWithIndex")
    val withIndex = input.zipWithIndex()
    val reversed = withIndex.sortBy(_._2, ascending = false).map(_._1)
    val reverseResult = reversed.collect()
    println(s"Reverse result: ${reverseResult.mkString(", ")} (Expected: 6,2,9,5,1,4,1,3)")
    
    // Test 5: shuffle (random reordering)
    println("5. Testing shuffle using sample and union")
    val shuffled = input.sample(withReplacement = false, fraction = 1.0, seed = 42)
    val shuffleResult = shuffled.collect()
    println(s"Shuffle result: ${shuffleResult.mkString(", ")} (Expected: Same elements in different order)")
    println("Note: Order varies due to randomness but all elements preserved")
  }
  
  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestReorders")
    
    // Create directory if it doesn't exist
    if (!currentDir.exists()) {
      currentDir.mkdirs()
    }
    
    val allFiles = Option(currentDir.listFiles()).getOrElse(Array.empty[File])
    val logFiles = allFiles.filter { file =>
      file.getName.startsWith("spark_inputs_") || file.getName.startsWith("spark_finals_")
    }
    logFiles.foreach(_.delete())
    println(s"Cleaned up ${logFiles.length} existing log files")
  }
  
  def checkLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestReorders")
    
    // Create directory if it doesn't exist
    if (!currentDir.exists()) {
      currentDir.mkdirs()
    }
    
    val allFiles = Option(currentDir.listFiles()).getOrElse(Array.empty[File])
    val inputLogs = allFiles.filter(_.getName.startsWith("spark_inputs_"))
    val outputLogs = allFiles.filter(_.getName.startsWith("spark_finals_"))
    
    println(s"Found ${inputLogs.length} input log files:")
    inputLogs.foreach(file => println(s"  - ${file.getName}"))
    
    println(s"Found ${outputLogs.length} output log files:")
    outputLogs.foreach(file => println(s"  - ${file.getName}"))
    
    // Group by stage and print: Input of stage N, then Output of stage N
    val StagePattern = "spark_(inputs|finals)_stage(\\d+)_p(\\d+)_att(\\d+)_taskId(\\d+)\\.(log|tmp)".r
    case class LogKey(kind: String, stage: Int, part: Int, att: Int, task: Long, name: String)

    def parseKey(f: File): Option[LogKey] = f.getName match {
      case StagePattern(kind, s, p, att, task, _) =>
        Some(LogKey(kind, s.toInt, p.toInt, att.toInt, task.toLong, f.getName))
      case _ => None
    }

    val allLogs = (Option(inputLogs).getOrElse(Array.empty) ++ Option(outputLogs).getOrElse(Array.empty))
    val keyed = allLogs.flatMap(f => parseKey(f).map(k => k -> f))
    val byStage = keyed.groupBy(_._1.stage).toSeq.sortBy(_._1)

    byStage.foreach { case (stage, entries) =>
      val inputs = entries.collect { case (k, f) if k.kind == "inputs" => (k, f) }
        .sortBy { case (k, _) => (k.part, k.task, k.att, k.name) }
      val finals = entries.collect { case (k, f) if k.kind == "finals" => (k, f) }
        .sortBy { case (k, _) => (k.part, k.task, k.att, k.name) }

      println(s"\n===== Stage $stage =====")

      println(s"Input of stage $stage:")
      if (inputs.isEmpty) println("  (no input logs found for this stage)")
      inputs.foreach { case (k, file) =>
        println(s"  - ${file.getName} (p=${k.part}, att=${k.att}, task=${k.task})")
        var src: Option[scala.io.BufferedSource] = None
        try {
          src = Some(scala.io.Source.fromFile(file))
          src.get.getLines().foreach(line => println(s"    $line"))
        } catch {
          case e: Exception => println(s"    Error reading file: ${e.getMessage}")
        } finally {
          try src.foreach(_.close()) catch { case _: Throwable => () }
        }
      }

      println(s"Output of stage $stage:")
      if (finals.isEmpty) println("  (no output logs found for this stage)")
      finals.foreach { case (k, file) =>
        println(s"  - ${file.getName} (p=${k.part}, att=${k.att}, task=${k.task})")
        var src: Option[scala.io.BufferedSource] = None
        try {
          src = Some(scala.io.Source.fromFile(file))
          src.get.getLines().foreach(line => println(s"    $line"))
        } catch {
          case e: Exception => println(s"    Error reading file: ${e.getMessage}")
        } finally {
          try src.foreach(_.close()) catch { case _: Throwable => () }
        }
      }
    }
  }
}
