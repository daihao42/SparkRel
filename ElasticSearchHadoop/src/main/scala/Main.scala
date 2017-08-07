import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object Main  {

	def sparkContextInit(url:String)(appName:String)(confset:Map[String,String]):SparkContext = {
		val conf = new SparkConf().setAppName(appName).setMaster(url)
		for (cs <- confset) conf.set(cs._1,cs._2)
		new SparkContext(conf)
	}

	def main(args: Array[String]): Unit = {

		//自动生成ID
		// val sc = sparkContextInit("spark://node1:7077")("Elasticsearch")(
			// Map("es.index.auto.create" -> "true",
					// "es.nodes" -> "192.168.40.97:9200"
					// )
		// )

		// val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
		// val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
		// sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

		//指定ID
		val sc = sparkContextInit("spark://node1:7077")("Elasticsearch")(
			Map(
					"es.nodes" -> "192.168.40.97,192.168.40.99,168.40.101",
					"es.port" -> "9200"
					)
		)
		// sc.setLogLevel("WARN")

		case class Trip(departure: String, arrival: String, id: String, rr: String)

		val upcomingTrip = Trip("o1", "SFO","101","1,2,3")
		val lastWeekTrip = Trip("o2", "OTP","102","1,2,3")
		val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
		EsSpark.saveToEs(rdd, "spark/docs",Map("es.mapping.id" -> "id"))

		//动态写入不同的块
		// val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
		// val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
		// val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
		//
		// sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")

		//指定ID
		// val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
		// val muc = Map("iata" -> "MUC", "name" -> "Munich")
		// val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
		//
		// val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
		// airportsRDD.saveToEsWithMeta("airports/2015")
		sc.stop()
	}

}
