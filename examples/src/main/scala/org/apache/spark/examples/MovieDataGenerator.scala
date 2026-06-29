package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Movie Data Generator
 * 
 * Generates two files:
 * 1. Movies file (~1.7 MB): movieid,imdbid,language,title
 * 2. Ratings file: userid,movieid,rating,timestamp
 * 
 * Usage:
 *   ./bin/run-example MovieDataGenerator [movies_output] [ratings_output] [num_movies] [num_ratings]
 * 
 * Defaults:
 *   movies_output:  /tmp/movies.txt
 *   ratings_output: /tmp/ratings.txt
 *   num_movies:     10000 (10K movies = ~1.7MB)
 *   num_ratings:    5000000 (5M ratings = ~100MB)
 */
object MovieDataGenerator {
  
  // Movie title components for realistic titles
  val adjectives = Array(
    "Dark", "Silent", "Lost", "Hidden", "Forgotten", "Secret", "Last", "First",
    "Eternal", "Infinite", "Final", "Ultimate", "Perfect", "Broken", "Ancient",
    "Modern", "Wild", "Dangerous", "Beautiful", "Mysterious", "Haunted", "Deadly"
  )
  
  val nouns = Array(
    "Night", "Dawn", "Storm", "Shadow", "Light", "Dream", "Legend", "Hero",
    "Warrior", "Kingdom", "Empire", "City", "World", "Journey", "Quest", "Battle",
    "War", "Peace", "Love", "Death", "Life", "Hope", "Destiny", "Fate", "Time",
    "Space", "Dragon", "Phoenix", "Wolf", "Tiger", "Lion", "Eagle", "Sword",
    "Crown", "Throne", "Ring", "Stone", "Key", "Door", "Gate", "Bridge", "Island"
  )
  
  val suffixes = Array(
    "", " Returns", " Rises", " Falls", " Begins", " Ends", " Forever",
    ": Part 1", ": Part 2", ": The Beginning", ": The End", ": Reloaded",
    ": Revolution", ": Resurrection", ": Redemption", " Strikes Back"
  )
  
  val languages = Array(
    "en", "en", "en", "en", "en", "en", "en", "en",  // English is most common
    "es", "fr", "de", "it", "pt", "ja", "ko", "zh", "ru", "ar", "hi"
  )
  
  val genres = Array(
    "Action", "Comedy", "Drama", "Thriller", "Horror", "Sci-Fi", "Fantasy",
    "Romance", "Mystery", "Adventure", "Crime", "Documentary", "Animation"
  )
  
  def main(args: Array[String]): Unit = {
    val moviesOutput = if (args.length > 0) args(0) else "/tmp/movies.txt"
    val ratingsOutput = if (args.length > 1) args(1) else "/tmp/ratings.txt"
    val numMovies = if (args.length > 2) args(2).toInt else 10000
    val numRatings = if (args.length > 3) args(3).toLong else 5000000L
    
    val conf = new SparkConf()
      .setAppName("Movie Data Generator")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    try {
      println("=" * 80)
      println("MOVIE DATA GENERATOR")
      println("=" * 80)
      println(s"Movies output:     $moviesOutput")
      println(s"Ratings output:    $ratingsOutput")
      println(s"Number of movies:  ${"%,d".format(numMovies)} (~${numMovies * 170 / 1024 / 1024}.${(numMovies * 170 / 1024) % 1024}MB)")
      println(s"Number of ratings: ${"%,d".format(numRatings)} (~${numRatings * 20 / 1024 / 1024}MB)")
      println(s"Partitions:        16")
      println("=" * 80)
      println()
      
      val startTime = System.currentTimeMillis()
      
      // PHASE 1: Generate Movies
      println("[PHASE 1] Generating movie data...")
      
      val movies = sc.parallelize(1 to numMovies, 16)
        .map { movieId =>
          val seed = movieId.toLong
          
          // Generate IMDB ID (tt followed by 7 digits)
          val imdbId = f"tt${Math.abs((seed * 2654435761L) % 10000000)}%07d"
          
          // Select language
          val language = languages(Math.abs((seed * 1103515245L) % languages.length).toInt)
          
          // Generate movie title
          val adjective = adjectives(Math.abs((seed * 48271L) % adjectives.length).toInt)
          val noun = nouns(Math.abs((seed * 69621L) % nouns.length).toInt)
          val suffix = suffixes(Math.abs((seed * 40692L) % suffixes.length).toInt)
          val genre = genres(Math.abs((seed * 25214L) % genres.length).toInt)
          
          val title = s"$adjective $noun$suffix"
          
          // Format: movieid,imdbid,language,title
          s"$movieId,$imdbId,$language,$title"
        }
      
      println(s"  Generated ${"%,d".format(numMovies)} movies")
      println(s"  Saving to: $moviesOutput")
      movies.saveAsTextFile(moviesOutput)
      
      val phase1Time = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"  Phase 1 completed in ${phase1Time}s")
      println()
      
      // PHASE 2: Generate Ratings
      println("[PHASE 2] Generating rating data...")
      val phase2Start = System.currentTimeMillis()
      
      val numUsers = 100000  // 100K users
      
      val ratings = sc.parallelize(1L to numRatings, 16)
        .map { ratingId =>
          val seed = ratingId
          
          // Generate user ID (1 to numUsers)
          val userId = 1 + Math.abs((seed * 2654435761L) % numUsers).toInt
          
          // Generate movie ID (1 to numMovies)
          // Some movies get more ratings (popular movies)
          val movieSeed = (seed * 1103515245L + 12345L)
          val movieId = 1 + Math.abs(movieSeed % (numMovies * numMovies)) / numMovies
          
          // Generate rating (1.0 to 5.0, in 0.5 increments)
          val ratingValue = {
            val r = Math.abs((seed * 48271L) % 10)
            (r / 2.0 + 1.0).min(5.0)
          }
          
          // Generate timestamp (2010 to 2024 in seconds since epoch)
          val baseTimestamp = 1262304000L  // 2010-01-01
          val timeRange = 14L * 365 * 24 * 3600  // 14 years
          val timestamp = baseTimestamp + Math.abs((seed * 69621L) % timeRange)
          
          // Format: userid,movieid,rating,timestamp
          s"$userId,$movieId,$ratingValue,$timestamp"
        }
      
      println(s"  Generated ${"%,d".format(numRatings)} ratings from ${"%,d".format(numUsers)} users")
      println(s"  Saving to: $ratingsOutput")
      ratings.saveAsTextFile(ratingsOutput)
      
      val phase2Time = (System.currentTimeMillis() - phase2Start) / 1000.0
      println(s"  Phase 2 completed in ${phase2Time}s")
      println()
      
      val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
      
      println("=" * 80)
      println("GENERATION COMPLETE")
      println("=" * 80)
      println(s"Movies saved to:   $moviesOutput")
      println(s"Ratings saved to:  $ratingsOutput")
      println(s"Total time:        ${totalTime}s")
      println("=" * 80)
      
      println()
      println("To use this dataset with MovieReviewAnalysis:")
      println(s"  ./bin/run-example MovieReviewAnalysis $moviesOutput $ratingsOutput")
      
    } finally {
      sc.stop()
    }
  }
}
