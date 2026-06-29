package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Soccer Player Data Generator
 * 
 * Generates a synthetic soccer players dataset and saves it to disk.
 * Output format: <player_id>,<player_name>,<country>,<position>,<age>,<club>
 * 
 * Usage:
 *   ./bin/run-example SoccerDataGenerator [output_path] [num_players]
 * 
 * Defaults:
 *   output_path: /tmp/soccer_data.txt
 *   num_players: 800000 (800K players = ~20MB)
 */
object SoccerDataGenerator {
  
  // Realistic country distribution (top soccer countries)
  val countries = Array(
    ("Brazil", 15), ("Argentina", 12), ("France", 10), ("Spain", 10),
    ("Germany", 9), ("Italy", 9), ("England", 8), ("Netherlands", 6),
    ("Portugal", 6), ("Belgium", 5), ("Croatia", 4), ("Uruguay", 4),
    ("Colombia", 4), ("Mexico", 4), ("Poland", 3), ("Denmark", 3),
    ("Switzerland", 3), ("Sweden", 3), ("Austria", 2), ("Czech Republic", 2),
    ("Turkey", 2), ("Ukraine", 2), ("Serbia", 2), ("Russia", 2),
    ("Greece", 2), ("Romania", 2), ("Norway", 2), ("Japan", 2),
    ("South Korea", 2), ("USA", 2), ("Canada", 1), ("Australia", 1),
    ("Nigeria", 1), ("Ghana", 1), ("Ivory Coast", 1), ("Senegal", 1),
    ("Egypt", 1), ("Morocco", 1), ("Tunisia", 1), ("Algeria", 1),
    ("Chile", 1), ("Peru", 1), ("Ecuador", 1), ("Paraguay", 1),
    ("Costa Rica", 1), ("Panama", 1), ("Honduras", 1), ("Jamaica", 1),
    ("China", 1), ("Iran", 1)
  )
  
  val positions = Array("GK", "CB", "LB", "RB", "CDM", "CM", "CAM", "LW", "RW", "ST")
  
  val firstNames = Array(
    "Marco", "Luca", "Diego", "Carlos", "Juan", "Luis", "David", "Miguel",
    "Antonio", "José", "Manuel", "Francisco", "Rafael", "Daniel", "Gabriel",
    "Pedro", "Fernando", "Javier", "Roberto", "Alejandro", "Sergio", "Jorge",
    "Ricardo", "Alberto", "Eduardo", "Felipe", "Andrés", "Pablo", "Cristiano",
    "Lionel", "Neymar", "Kylian", "Mohamed", "Erling", "Robert", "Kevin",
    "Eden", "Luka", "Ivan", "Mateo", "Leonardo", "Thiago", "Bruno", "João"
  )
  
  val lastNames = Array(
    "Silva", "Santos", "Oliveira", "Pereira", "Costa", "Rodrigues", "Martins",
    "Sousa", "Fernandes", "Alves", "Gonçalves", "Lopez", "Martinez", "Garcia",
    "Rodriguez", "Hernandez", "Gonzalez", "Perez", "Sanchez", "Ramirez",
    "Torres", "Flores", "Rivera", "Gomez", "Diaz", "Cruz", "Morales", "Reyes",
    "Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner",
    "Becker", "Schulz", "Hoffmann", "Schäfer", "Koch", "Bauer", "Richter",
    "Klein", "Wolf", "Schröder", "Neumann", "Schwarz", "Zimmermann", "Braun"
  )
  
  val clubs = Array(
    "Real Madrid", "Barcelona", "Bayern Munich", "Manchester City", "Liverpool",
    "Paris Saint-Germain", "Juventus", "Chelsea", "Manchester United", "Arsenal",
    "Inter Milan", "AC Milan", "Atletico Madrid", "Borussia Dortmund", "Napoli",
    "Tottenham", "Roma", "Ajax", "Benfica", "Porto", "Sevilla", "Valencia",
    "RB Leipzig", "Bayer Leverkusen", "Marseille", "Lyon", "Monaco", "Lille"
  )
  
  def main(args: Array[String]): Unit = {
    val outputPath = if (args.length > 0) args(0) else "/tmp/soccer_data.txt"
    val numPlayers = if (args.length > 1) args(1).toInt else 800000
    
    val conf = new SparkConf()
      .setAppName("Soccer Data Generator")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    try {
      println("=" * 80)
      println("SOCCER PLAYER DATA GENERATOR")
      println("=" * 80)
      println(s"Output path:       $outputPath")
      println(s"Number of players: ${"%,d".format(numPlayers)} (~${numPlayers * 25 / 1024 / 1024}MB)")
      println(s"Countries:         ${countries.length}")
      println(s"Positions:         ${positions.length}")
      println(s"Partitions:        16")
      println("=" * 80)
      println()
      
      val startTime = System.currentTimeMillis()
      
      println("[PHASE 1] Generating soccer player data...")
      
      // Create weighted country distribution
      val countryWeights = countries.flatMap { case (country, weight) =>
        Array.fill(weight)(country)
      }
      val totalWeight = countryWeights.length
      
      val players = sc.parallelize(1 to numPlayers, 16)
        .map { playerId =>
          val seed = playerId
          
          // Select country based on weighted distribution
          val countryIdx = Math.abs((seed * 2654435761L) % totalWeight).toInt
          val country = countryWeights(countryIdx)
          
          // Generate player details
          val firstName = firstNames(Math.abs((seed * 1103515245L) % firstNames.length).toInt)
          val lastName = lastNames(Math.abs((seed * 12345L) % lastNames.length).toInt)
          val playerName = s"$firstName $lastName"
          
          val position = positions(Math.abs((seed * 48271L) % positions.length).toInt)
          val age = 18 + Math.abs((seed * 69621L) % 20).toInt  // Age 18-37
          val club = clubs(Math.abs((seed * 40692L) % clubs.length).toInt)
          
          // CSV format: player_id,player_name,country,position,age,club
          s"$playerId,$playerName,$country,$position,$age,$club"
        }
      
      println(s"  Generated ${"%,d".format(numPlayers)} players")
      println()
      
      println("[PHASE 2] Saving to disk...")
      players.saveAsTextFile(outputPath)
      
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000.0
      
      println()
      println("=" * 80)
      println("GENERATION COMPLETE")
      println("=" * 80)
      println(s"Output saved to:  $outputPath")
      println(s"Total time:       ${totalTime}s")
      println("=" * 80)
      
      println()
      println("To use this dataset with SoccerPlayerAnalysis:")
      println(s"  ./bin/run-example SoccerPlayerAnalysis $outputPath")
      
    } finally {
      sc.stop()
    }
  }
}
