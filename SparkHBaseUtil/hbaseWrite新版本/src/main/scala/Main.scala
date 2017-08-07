import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object Main  {

	def sparkContextInit(url:String)(appName:String):SparkContext = new SparkContext(new SparkConf().setAppName(appName).setMaster(url))

	def hbaseConfInit(quorum:String)(port:String)(tablename:String)(sc:SparkContext) = {
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", port)
		sc.hadoopConfiguration.set("hbase.master", "node1:600000")
		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
	}

	def jobInit(sc:SparkContext) = {
		val job = new Job(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		job
	}

	def main(args: Array[String]): Unit = {

		val sc = sparkContextInit("spark://node1:7077")("HBase")

		hbaseConfInit("node1,node2,node3")("2181")("test")(sc)

		val indataRDD = sc.makeRDD(Array("sss,hsdre,22323","3232323,y,4343","444,si44,14446"))
		val rdd = indataRDD.map(_.split(',')).map{arr=>{
			val put = new Put(Bytes.toBytes(arr(0)))
			put.add(Bytes.toBytes("clos"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
			put.add(Bytes.toBytes("clos"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
			(new ImmutableBytesWritable, put)
		}}

		rdd.saveAsNewAPIHadoopDataset(jobInit(sc).getConfiguration())

    sc.stop()
	}


}
