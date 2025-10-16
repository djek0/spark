package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestCollapsers {
  def main(args: Array[String]): Unit = {
    println("=== Testing Category 4: N→1 (Collapsers) ===")
    
    val spark = SparkSession.builder()
      .appName("TestCollapsers")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      testCollapsers(sc)
      
      // Check log files after test
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== Test Collapsers Complete ===")
  }
  
  def testCollapsers(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing N→1 transformations (special case - not per-element anymore)")
    
    val input = sc.parallelize(1 to 8, 2) // 2 partitions: [1,2,3,4] and [5,6,7,8]
    
    // Test 1: glom
    println("1. Testing glom() - N elements → 1 array per partition")
    val glommed = input.glom()
    val glomResult = glommed.collect()
    println(s"Glom result: ${glomResult.map(_.mkString("[", ",", "]")).mkString(", ")} (Expected: [1,2,3,4], [5,6,7,8])")
    println("Note: This breaks per-element UID tracking - each partition becomes one element")
    
    // Test 2: coalesce (reduces number of partitions)
    println("2. Testing coalesce() - multiple partitions → fewer partitions")
    val coalesced = input.coalesce(1)
    val coalescedResult = coalesced.collect()
    println(s"Coalesce result: ${coalescedResult.mkString(", ")} (Expected: 1,2,3,4,5,6,7,8)")
    println("Note: Elements are preserved but partition structure changes")
    
    // Test 3: repartition (changes partition structure)
    println("3. Testing repartition() - redistributes elements across partitions")
    val repartitioned = input.repartition(3)
    val repartitionedResult = repartitioned.collect()
    println(s"Repartition result: ${repartitionedResult.mkString(", ")} (Expected: 1,2,3,4,5,6,7,8 in different order)")
    println("Note: Elements are preserved but order and partition assignment may change")
    
    // Test 4: groupBy (groups elements by key)
    println("4. Testing groupBy() - groups elements by key function")
    val grouped = input.groupBy(_ % 3)
    val groupedResult = grouped.collect().map { case (k, v) => s"$k -> [${v.mkString(",")}]" }
    println(s"GroupBy result: ${groupedResult.mkString(", ")} (Expected: 0 -> [3,6], 1 -> [1,4,7], 2 -> [2,5,8])")
    println("Note: This creates key-value pairs where values are iterables")
  }
  
  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestCollapsers")
    
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
    val currentDir = new File(s"$userHome/spark/spark-trace/TestCollapsers")
    
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
