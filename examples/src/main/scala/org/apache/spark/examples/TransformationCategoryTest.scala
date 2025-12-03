package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

import java.io.File

object TransformationCategoryTest {
  def main(args: Array[String]): Unit = {
    println("=== Starting Transformation Category Test ===")

    val spark = SparkSession.builder()
      .appName("TransformationCategoryTest")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()

      println("\n=== Category 1: 1→0/1 (Droppers) ===")
      testDroppers(sc)

      println("\n=== Category 2: 1→M (Expanders) ===")
      testExpanders(sc)

      println("\n=== Category 3: 1→1 (Safe Pass-through) ===")
      testSafeTransformations(sc)

      println("\n=== Category 4: N→1 (Collapsers) ===")
      testCollapsers(sc)

      println("\n=== Category 5: 1→1 (Reorders) ===")
      testReorders(sc)

      // Check log files after each category
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()

    } finally {
      spark.stop()
    }

    println("\n=== Transformation Category Test Complete ===")
  }

  def testDroppers(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→0/1 transformations (should need UID queue management)")

    val input = sc.parallelize(1 to 10, 2)

    // Test 1: filter (already implemented)
    println("1. Testing filter() - should show UID queue logs")
    val filtered = input.filter(_ > 5)
    val filterResult = filtered.collect()
    println(s"Filter result: ${filterResult.mkString(", ")} (Expected: 6,7,8,9,10)")

    // Test 2: sample (withReplacement = false)
    println("2. Testing sample() - should behave like filter")
    val sampled = input.sample(withReplacement = false, fraction = 0.5, seed = 42)
    val sampleResult = sampled.collect()
    println(s"Sample result: ${sampleResult.mkString(", ")} (Expected: Variable due to randomness)")

    // Test 3: Multiple filters (chain of droppers)
    println("3. Testing chained filters")
    val chainFiltered = input
      .filter(_ > 3)
      .filter(_ < 8)
      .filter(_ % 2 == 0)
    val chainResult = chainFiltered.collect()
    println(s"Chain filter result: ${chainResult.mkString(", ")} (Expected: 4,6)")
  }

  def testExpanders(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→M transformations (should need UID queue management)")

    val input = sc.parallelize(1 to 5, 2)

    // Test 1: flatMap
    println("1. Testing flatMap() - should show expansion behavior")
    val flatMapped = input.flatMap(x => List(x, x * 10))
    val flatMapResult = flatMapped.collect()
    println(s"FlatMap result: ${flatMapResult.mkString(", ")} (Expected: 1,10,2,20,3,30,4,40,5,50)")

    // Test 2: flatMapValues (on pair RDD)
    println("2. Testing flatMapValues() - should expand values")
    val pairRDD = input.map(x => (x % 3, x))
    val flatMapValues = pairRDD.flatMapValues(v => List(v, v * 2))
    val flatMapValuesResult = flatMapValues.collect()
    println(s"FlatMapValues result: ${flatMapValuesResult.mkString(", ")} (Expected: (0,3),(0,6),(1,1),(1,2),(2,2),(2,4),(0,6),(0,12),(1,4),(1,8))")

    // Test 3: flatMap with variable expansion
    println("3. Testing flatMap with variable expansion")
    val variableExpansion = input.flatMap(x => (1 to x).toList)
    val variableResult = variableExpansion.collect()
    println(s"Variable expansion result: ${variableResult.mkString(", ")} (Expected: 1,1,2,1,2,3,1,2,3,4,1,2,3,4,5)")
  }

  def testSafeTransformations(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→1 transformations (should NOT need UID queue management)")

    val input = sc.parallelize(1 to 5, 2)

    // Test 1: map
    println("1. Testing map() - should be pass-through")
    val mapped = input.map(_ * 2)
    val mapResult = mapped.collect()
    println(s"Map result: ${mapResult.mkString(", ")} (Expected: 2,4,6,8,10)")

    // Test 2: mapValues (on pair RDD)
    println("2. Testing mapValues() - should be pass-through")
    val pairRDD = input.map(x => (x % 3, x))
    val mapValues = pairRDD.mapValues(_ * 3)
    val mapValuesResult = mapValues.collect()
    println(s"MapValues result: ${mapValuesResult.mkString(", ")} (Expected: (1,3),(2,6),(0,9),(1,12),(2,15))")

    // Test 3: keyBy
    println("3. Testing keyBy() - should be pass-through")
    val keyBy = input.keyBy(_ % 2)
    val keyByResult = keyBy.collect()
    println(s"KeyBy result: ${keyByResult.mkString(", ")} (Expected: (1,1),(0,2),(1,3),(0,4),(1,5))")

    // Test 4: values and keys
    println("4. Testing values() and keys() - should be pass-through")
    val values = keyBy.values
    val keys = keyBy.keys
    val valuesResult = values.collect()
    val keysResult = keys.collect()
    println(s"Values result: ${valuesResult.mkString(", ")} (Expected: 1,2,3,4,5)")
    println(s"Keys result: ${keysResult.mkString(", ")} (Expected: 1,0,1,0,1)")
  }

  def testCollapsers(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing N→1 transformations (special case - not per-element anymore)")

    val input = sc.parallelize(1 to 8, 2) // 2 partitions: [1,2,3,4] and [5,6,7,8]

    // Test 1: glom
    println("1. Testing glom() - N elements → 1 array per partition")
    val glommed = input.glom()
    val glomResult = glommed.collect()
    println(s"Glom result: ${glomResult.map(_.mkString("[", ",", "]")).mkString(", ")} (Expected: [1,2,3,4],[5,6,7,8])")
    println("Note: This breaks per-element UID tracking - each partition becomes one element")
  }

  def testReorders(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→1 reordering transformations (same count, different order)")

    val input = sc.parallelize(List(3, 1, 4, 1, 5, 9, 2, 6), 2)

    // Test 1: sortWithinPartitions
    println("1. Testing sortWithinPartitions() - reorders within each partition")
    val sorted = input.sortBy(identity)
    val sortResult = sorted.collect()
    println(s"Sort result: ${sortResult.mkString(", ")} (Expected: 1,1,2,3,4,5,6,9)")
    println("Note: This preserves count but changes order - UID pairing gets complex")
  }

  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TransformationCategoryTest")

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
    val currentDir = new File(s"$userHome/spark/spark-trace/TransformationCategoryTest")

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