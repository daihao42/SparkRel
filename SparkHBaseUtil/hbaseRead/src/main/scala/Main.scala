import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Result,Scan,Get}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat,TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import scala.collection.JavaConverters._
object Main  {

	def sparkContextInit(url:String)(appName:String):SparkContext = new SparkContext(new SparkConf().setAppName(appName).setMaster(url))

	def hbaseConfInit(quorum:String)(port:String) = {
		val conf = HBaseConfiguration.create()
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", port)
		conf.set("hbase.master", "node1:600000")
		//返回conf
		conf
	}

	def main(args: Array[String]): Unit = {

		val sc = sparkContextInit("spark://node1:7077")("HBase")


		val conf = hbaseConfInit("node1,node2,node3")("2181")

		val tablename = "test"

		conf.set(TableInputFormat.INPUT_TABLE, tablename)

		val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
		  classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
		  classOf[org.apache.hadoop.hbase.client.Result])

		val count = usersRDD.count()
		println("-----------------------------------------------")
		println("Users RDD Count:" + count)
		println("-----------------------------------------------")
		usersRDD.cache()

		//遍历输出
		println("-----------------------------------------------")
		usersRDD.map(tuple => tuple._2).map(result => (result.getRow,
														result.getColumn("clos".getBytes(),
														"name".getBytes()))).map(row => {
		(
		  row._1.map(_.toChar).mkString,
		  row._2.asScala.reduceLeft {
		    (a, b) => if (a.getTimestamp > b.getTimestamp) a else b
		  }.getValue.map(_.toChar).mkString
		)
		}).take(10) .map(println)
		println("-----------------------------------------------")


	}


}
