package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Application 2: Soccer Player Analysis
 * 
 * Takes as input a file containing soccer player data with the structure:
 * <player_id>,<player_name>,<country>,<position>,<age>,<club>
 * 
 * Purpose: Find the number of soccer players from each country and rank
 * the countries in descending order based on this count.
 * 
 * Usage:
 *   ./bin/run-example SoccerPlayerAnalysis [partitions] [input_file] [output_dir]
 * 
 * Parameters:
 *   partitions:  Number of partitions for Spark operations (default: auto-detect)
 *   input_file:  Path to soccer player data file (required)
 *   output_dir:  Optional output directory for results
 * 
 * Examples:
 *   ./bin/run-example SoccerPlayerAnalysis 16 /tmp/soccer_data.txt
 *   ./bin/run-example SoccerPlayerAnalysis 8 /tmp/soccer_data.txt /tmp/output
 * 
 * To generate test data:
 *   ./bin/run-example SoccerDataGenerator /tmp/soccer_data.txt
 */
object SoccerPlayerAnalysis {
  
  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val partitions = if (args.length > 0 && args(0).forall(_.isDigit)) {
      args(0).toInt
    } else {
      -1  // Auto-detect
    }
    
    val inputFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 1) Some(args(1)) else None
    } else {
      if (args.length > 0) Some(args(0)) else None
    }
    
    val outputDir = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 2) Some(args(2)) else None
    } else {
      if (args.length > 1) Some(args(1)) else None
    }
    
    val conf = new SparkConf()
      .setAppName("Soccer Player Analysis")
      .setMaster("local[*]")
    
    // Set default parallelism if specified
    if (partitions > 0) {
      conf.set("spark.default.parallelism", partitions.toString)
    }
    
    val sc = new SparkContext(conf)
    
    try {
      val startTime = System.currentTimeMillis()
      
      // Validate input
      if (inputFile.isEmpty) {
        println("ERROR: Input file is required!")
        println("")
        println("Usage: ./bin/run-example SoccerPlayerAnalysis [partitions] [input_file] [output_dir]")
        println("")
        println("To generate test data:")
        println("  ./bin/run-example SoccerDataGenerator /tmp/soccer_data.txt")
        println("")
        System.exit(1)
      }
      
      // Get Spark configuration info
      val defaultParallelism = sc.defaultParallelism
      val masterUrl = sc.master
      val numExecutors = sc.getExecutorMemoryStatus.size - 1  // Subtract driver
      
      println("=" * 80)
      println("SOCCER PLAYER ANALYSIS - Application 2")
      println("=" * 80)
      println(s"Spark Master:         $masterUrl")
      println(s"Default Parallelism:  $defaultParallelism partitions")
      println(s"Executors:            $numExecutors")
      println(s"Input File:           ${inputFile.get}")
      if (outputDir.isDefined) {
        println(s"Output Directory:     ${outputDir.get}")
      }
      println("=" * 80)
      println()
      
      // Load data
      println(s"[PHASE 1] Loading soccer player data from: ${inputFile.get}")
      val minPartitions = if (partitions > 0) partitions else 16
      val players = sc.textFile(inputFile.get, minPartitions = minPartitions)
        .map { line =>
          val parts = line.split(",")
          if (parts.length >= 6) {
            // Extract: player_id, player_name, country, position, age, club
            val playerId = parts(0).trim
            val playerName = parts(1).trim
            val country = parts(2).trim
            val position = parts(3).trim
            val age = if (parts(4).trim.forall(_.isDigit)) parts(4).trim.toInt else 0
            val club = parts(5).trim
            (playerId, playerName, country, position, age, club)
          } else {
            ("invalid", "", "", "", 0, "")
          }
        }
        .filter { case (id, _, _, _, _, _) => id != "invalid" }
      
      val actualPartitions = players.getNumPartitions
      println(s"  Data loaded into $actualPartitions partitions")
      
      val totalPlayers = players.count()
      
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"  Total players: ${"%,d".format(totalPlayers)}")
      println(s"  Load time: ${loadTime}s")
      println()
      
      // PHASE 2: Count players per country
      println("[PHASE 2] Counting players per country (triggers Byzantine verification)...")
      val phase2Start = System.currentTimeMillis()
      
      val countryPlayerCounts = players
        .map { case (_, _, country, _, _, _) => (country, 1) }
        .reduceByKey(_ + _)  // Shuffle + aggregation - verification point!

      // Force execution
      val totalCountries = countryPlayerCounts.count()
      
      val phase2Time = (System.currentTimeMillis() - phase2Start) / 1000.0
      println(s"  Aggregation completed: $totalCountries unique countries")
      println(s"  Time: ${phase2Time}s")
      println()
      
      // PHASE 3: Collect and rank countries by player count
      println("[PHASE 3] Collecting and ranking countries...")
      val phase3Start = System.currentTimeMillis()
      
      // Collect all results to driver (safe at the end of job)
      // Then sort in driver memory to avoid sortByKey's UID tracking issues
      val allCountries = countryPlayerCounts.collect().sortBy(-_._2)
      
      val phase3Time = (System.currentTimeMillis() - phase3Start) / 1000.0
      println(s"  Collection and ranking completed")
      println(s"  Time: ${phase3Time}s")
      println()
      
      // Display results
      println("=" * 80)
      println("COUNTRY RANKINGS BY NUMBER OF PLAYERS")
      println("=" * 80)
      println(f"${"Rank"}%-6s ${"Country"}%-25s ${"Players"}%15s ${"Percentage"}%12s")
      println("-" * 80)
      
      allCountries.zipWithIndex.foreach { case ((country, count), idx) =>
        val rank = idx + 1
        val percentage = (count.toDouble / totalPlayers) * 100
        println(f"$rank%-6d $country%-25s ${"%,d".format(count)}%15s ${percentage}%11.2f%%")
      }
      println("-" * 80)
      println(f"${"TOTAL"}%-6s ${""}%-25s ${"%,d".format(totalPlayers)}%15s ${100.0}%11.2f%%")
      println("=" * 80)
      println()
      
      // Additional statistics
      println("=" * 80)
      println("STATISTICS")
      println("=" * 80)
      val topCountry = allCountries.head
      val leastCountry = allCountries.last
      val avgPlayersPerCountry = totalPlayers.toDouble / totalCountries
      
      println(s"Top country:              ${topCountry._1} (${"%,d".format(topCountry._2)} players)")
      println(s"Least represented:        ${leastCountry._1} (${"%,d".format(leastCountry._2)} players)")
      println(s"Average players/country:  ${"%,.1f".format(avgPlayersPerCountry)}")
      println(s"Total countries:          $totalCountries")
      println(s"Total players:            ${"%,d".format(totalPlayers)}")
      println("=" * 80)
      println()
      
      // Save results if output directory specified
      outputDir.foreach { dir =>
        println(s"[OUTPUT] Saving results to: $dir")
        sc.parallelize(allCountries).saveAsTextFile(dir)
        println(s"  Results saved successfully")
        println()
      }
      
      val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
      
      println("=" * 80)
      println("EXECUTION SUMMARY")
      println("=" * 80)
      println(f"Total players analyzed:  ${"%,d".format(totalPlayers)}")
      println(f"Total countries:         $totalCountries")
      println(f"Phase 1 (Load):          ${loadTime}%.2fs")
      println(f"Phase 2 (Count):         ${phase2Time}%.2fs")
      println(f"Phase 3 (Rank):          ${phase3Time}%.2fs")
      println(f"Total execution time:    ${totalTime}%.2fs")
      println("=" * 80)
      
    } finally {
      sc.stop()
    }
  }
}
