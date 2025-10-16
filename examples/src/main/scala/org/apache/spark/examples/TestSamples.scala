package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestSamples {
  def main(args: Array[String]): Unit = {
    println("=== Testing All Sampling Operations ===")
    
    val spark = SparkSession.builder()
      .appName("TestSamples")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      val input = sc.parallelize(1 to 10, 2) // 2 partitions: [1,2,3,4,5] and [6,7,8,9,10]
      println(s"Input data: ${input.collect().mkString(", ")}")
      
      // Test 1: sample() with Bernoulli sampler (withReplacement = false)
      println("\n1. Testing sample() with Bernoulli sampler (withReplacement = false)")
      val bernoulliSample = input.sample(withReplacement = false, fraction = 0.5, seed = 42)
      val bernoulliResult = bernoulliSample.collect()
      println(s"Bernoulli sample result: ${bernoulliResult.mkString(", ")} (Expected: ~5 elements, no duplicates)")
      
      // Test 2: sample() with Poisson sampler (withReplacement = true)
      println("\n2. Testing sample() with Poisson sampler (withReplacement = true)")
      val poissonSample = input.sample(withReplacement = true, fraction = 0.8, seed = 42)
      val poissonResult = poissonSample.collect()
      println(s"Poisson sample result: ${poissonResult.mkString(", ")} (Expected: ~8 elements, may have duplicates)")
      
      // Test 3: randomSampleWithRange()
      println("\n3. Testing randomSampleWithRange()")
      val rangeSample = input.randomSampleWithRange(0.2, 0.7, seed = 42)
      val rangeResult = rangeSample.collect()
      println(s"Range sample result: ${rangeResult.mkString(", ")} (Expected: elements in range [0.2, 0.7))")
      
      // Test 4: takeSample() - this is an action, not a transformation
      println("\n4. Testing takeSample() - action that returns Array")
      val takeSampleResult = input.takeSample(withReplacement = false, num = 4, seed = 42)
      println(s"TakeSample result: ${takeSampleResult.mkString(", ")} (Expected: exactly 4 elements)")
      
      // Test 5: takeSample() with replacement
      println("\n5. Testing takeSample() with replacement")
      val takeSampleReplResult = input.takeSample(withReplacement = true, num = 6, seed = 42)
      println(s"TakeSample with replacement result: ${takeSampleReplResult.mkString(", ")} (Expected: 6 elements, may have duplicates)")
      
      // Test 6: randomSplit()
      println("\n6. Testing randomSplit()")
      val splits = input.randomSplit(Array(0.3, 0.4, 0.3), seed = 42)
      val splitResults = splits.map(_.collect())
      println(s"RandomSplit results:")
      splitResults.zipWithIndex.foreach { case (split, idx) =>
        println(s"  Split $idx: ${split.mkString(", ")} (${split.length} elements)")
      }
      
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
  }
  
  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestSamples")
    
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
    val currentDir = new File(s"$userHome/spark/spark-trace/TestSamples")
    
    // Create directory if it doesn't exist
    if (!currentDir.exists()) {
      currentDir.mkdirs()
    }
    
    val allFiles = Option(currentDir.listFiles()).getOrElse(Array.empty[File])
    val inputLogs = allFiles.filter(_.getName.startsWith("spark_inputs_"))
    val outputLogs = allFiles.filter(_.getName.startsWith("spark_finals_"))
    
    println(s"Found ${inputLogs.length} input log files:")
    inputLogs.foreach(f => println(s"  - ${f.getName}"))
    
    println(s"Found ${outputLogs.length} output log files:")
    outputLogs.foreach(f => println(s"  - ${f.getName}"))
    
    // Group logs by stage and display per-stage results
    val stageGroups = (inputLogs ++ outputLogs).groupBy { file =>
      val name = file.getName
      val stagePattern = """stage(\d+)""".r
      stagePattern.findFirstMatchIn(name).map(_.group(1).toInt).getOrElse(-1)
    }.toSeq.sortBy(_._1).filter(_._1 >= 0)
    
    stageGroups.foreach { case (stageId, files) =>
      println(s"\n===== Stage $stageId =====")
      
      val stageInputs = files.filter(_.getName.startsWith("spark_inputs_")).sortBy(_.getName)
      val stageOutputs = files.filter(_.getName.startsWith("spark_finals_")).sortBy(_.getName)
      
      if (stageInputs.nonEmpty) {
        println(s"Input of stage $stageId:")
        stageInputs.foreach { file =>
          val partitionPattern = """_p(\d+)_att(\d+)_taskId(\d+)""".r
          val matches = partitionPattern.findFirstMatchIn(file.getName)
          val (p, att, task) = matches.map(m => (m.group(1), m.group(2), m.group(3))).getOrElse(("?", "?", "?"))
          
          println(s"  - ${file.getName} (p=$p, att=$att, task=$task)")
          if (file.exists() && file.length() > 0) {
            val lines = scala.io.Source.fromFile(file).getLines().toList
            lines.foreach(line => println(s"    $line"))
          }
        }
      }
      
      if (stageOutputs.nonEmpty) {
        println(s"Output of stage $stageId:")
        stageOutputs.foreach { file =>
          val partitionPattern = """_p(\d+)_att(\d+)_taskId(\d+)""".r
          val matches = partitionPattern.findFirstMatchIn(file.getName)
          val (p, att, task) = matches.map(m => (m.group(1), m.group(2), m.group(3))).getOrElse(("?", "?", "?"))
          
          println(s"  - ${file.getName} (p=$p, att=$att, task=$task)")
          if (file.exists() && file.length() > 0) {
            val lines = scala.io.Source.fromFile(file).getLines().toList
            lines.foreach(line => println(s"    $line"))
          }
        }
      }
    }
    
    println("\n=== Test Samples Complete ===")
  }
}
