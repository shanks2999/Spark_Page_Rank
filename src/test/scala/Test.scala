
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}


class Test extends FlatSpec {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Shanks Test")
    .config("spark.master", "local")
    .getOrCreate()
  val hadoopConfiguration = new Configuration()
  hadoopConfiguration.set("xmlinput.start", "<article>|<inproceedings>")
  hadoopConfiguration.set("xmlinput.end", "</article>|</inproceedings>")
  hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", "input/test.xml")
  val jSparkContext = new JavaSparkContext(sparkSession.sparkContext)
  val tag = jSparkContext.newAPIHadoopRDD(hadoopConfiguration, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  val professor_map = tag.rdd.flatMap{ s =>
    //      val document = StringEscapeUtils.unescapeHtml4(s._2.toString)       // Uncomment if you want to remove unnecessary accesnts/ditrics
    val document = s._2.toString
    val myPair = PageRank.getProfessor(document)
    myPair.toList
  }
    .distinct().groupByKey().cache()


    assert(professor_map != null)


}
