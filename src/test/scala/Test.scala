import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
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
    val document = s._2.toString
    val myPair = PageRank.getProfessor(document)
    myPair.toList
  }
    .distinct().groupByKey().cache()

  /**
    * Checking is the map returned exists or not and was able to read the Test.xml data
    **/
  assert(professor_map != null)


  /**
    * Checking getProfessor method by passing  sample XML string
    * depending on the permutation the output should be 4
    **/
  val myPair = PageRank.getProfessor("<article>\n<author>Sanjeev Saxena</author>\n<author>Shashank Maithani</author>\n<journal>Acta Inf.</journal>\n</article>")
  assert(myPair.toList.size == 4)

  /**
    * Verifying the logic of getRank() method which gives Ranked RDD
    * sorted in descending order of ranks
    * Based on the test.xml the total nodes on graph should be 10, highest ranked is 'Sanjeev Saxena' with rank '0.4355402787824717'
    **/
  val rank = PageRank.getRanks(professor_map)
  assert(rank != null)
  assert(rank.collect().size > 0)
  assert(rank.count() == 10)
  assert(rank.collect()(0)._1.equals("Sanjeev Saxena"))
  assert(rank.collect()(0)._2 == 0.4355402787824717)

}