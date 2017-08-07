import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{StreamingContext,Seconds,Minutes}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.InputDStream
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer

object Main  {

	def sparkStreamingInit(url:String)(appName:String)(sec:Int):StreamingContext = new StreamingContext(new SparkConf().setAppName(appName).setMaster(url),Seconds(sec))

	def wordCounts(data: InputDStream[(String,String)]) = data.map(t => t._2).
															flatMap(_.split(" ")).
															map((_, 1)).
															reduceByKeyAndWindow(_ + _ ,_ - _ ,Seconds(360),Seconds(30))

	def kafkaProducerConfig(brokers:String) = {
    	val p = new Properties()
    	p.setProperty("bootstrap.servers", brokers)
    	p.setProperty("key.serializer", classOf[StringSerializer].getName)
    	p.setProperty("value.serializer", classOf[StringSerializer].getName)
    	p
  	}

	def main(args: Array[String]): Unit = {

		val ssc = sparkStreamingInit("spark://node1:7077")("SparkKafka")(10)

		ssc.checkpoint("checkpoint")

		val brokers = "192.168.40.97:9092,192.168.40.99:9092,192.168.40.101:9092"
		val topics = "sparktest"
		val outTopics = "result"

		// 广播KafkaSink
		val kafkaProducer: Broadcast[KafkaSink[String, String]] = ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig(brokers)))

		// Create a direct stream
		wordCounts(KafkaReader.Read(ssc,brokers,topics)).
							foreachRDD(rdd => {
									if (!rdd.isEmpty) {
											rdd.foreach(record => {
											kafkaProducer.value.send(outTopics, record._1+":"+record._2.toString)
											// do something else
										})
									}
								})

		ssc.start()
		ssc.awaitTermination()
	}


}
