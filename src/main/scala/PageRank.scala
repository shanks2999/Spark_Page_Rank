import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext, function}
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
//import org.apache.commons.lang3.StringUtils
//import org.apache.commons.text.StringEscapeUtils
import com.typesafe.scalalogging.Logger

object PageRank extends App {

  def getProfessor(s: String):ListBuffer[(String, String)]={
    val document = s.toString
    var professors = new ListBuffer[String]()
    var node = ""
    val lines = document.split("\n")
    for (line <- lines) {
      if (line.startsWith("<author")) {
        val name = line.substring(line.indexOf(">") + 1, line.lastIndexOf("</author>"))
        professors += name
      }
      if (line.startsWith("<journal")) {
        val name = line.substring(line.indexOf(">") + 1, line.lastIndexOf("</journal>"))
        node = name
      }
      if (line.startsWith("<booktitle")) {
        val name = line.substring(line.indexOf(">") + 1, line.lastIndexOf("</booktitle>"))
        node = name
      }
    }
    var pair =  new ListBuffer[(String, String)]()
    //      (journal, professors.toList)
    for(x<-professors) {
      pair += ((x, node))
      for(y<-professors) {
        if(!x.equals(y)){
          pair += ((x,y))
        }
      }
    }
    pair
  }

  def getRanks(professor_map : RDD[(String, Iterable[String])]): RDD[(String, Double)]={
    var ranks = professor_map.mapValues(v => 1.0)

    //ranks.collect().foreach(println)        //Print initial ranks

    /**
      * Now running the PageRank upto specified number
      * or iterations. Hopefully it converges
      **/
    for (i <- 1 to 5) {
      val network = professor_map.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = network.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    val sorted_descending = ranks.sortBy(_._2, false)       // Sorting ranks in descending order
    //  sorted_descending.collect().foreach(println)                //Print updated ranks
    return sorted_descending
  }
//  def main(args :Array[String]): Unit={
/**
  *
  * Experimental Code which I played around with
  * using data bricks framework
  * Could be useful sometime in future :D

    //    val conf = new SparkConf()
    //    conf.setMaster("local")
    //    conf.setAppName("ShanksApp")
    //
    //    val sc = new SparkContext(conf)

    //    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    //    rdd1.collect().foreach(println)
    //
    //    val text = sc.textFile("input/data")
    //    val counts = text.flatMap(line => line.split(" ")
    //    ).map(word => (word,1)).reduceByKey(_+_)
    ////    counts.collect
    //    counts.saveAsTextFile("output")
    //    println(counts);
    //    spark.read.format("com.databricks.spark.xml").options(rowTag="book").load("sample/xml/")
    //    val sqlContext  :SQLContext = new org.apache.spark.sql.SQLContext(sc)


    //    val article_df = spark.read.format("com.databricks.spark.xml")
    //      .option("inferSchema", true)
    ////      .option("mode", "DROPMALFORMED")
    ////      .option("rootTag","dblp")
    //      .option("rowTag","article")
    //      .load("input/dblp.xml")
    //    print(article_df.show())

    //    val df = spark.read
    //      .option("rowTag", "article")
    //      .xml("input/hmm.xml")
    //
    //    val selectedData = df.select("author", "_id")
    //    selectedData.write
    //      .option("rootTag", "books")
    //      .option("rowTag", "book")
    //      .xml("dblp.xml")

    //    val sparkConfiguration = new SparkConf().setAppName("XML Reading")
    //    sparkConfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .setMaster("local")
  *
  *
**/

    val LOG = Logger("SMAITH2")
    LOG.info("Starting Spark App")

    /**
      * Setting up Hadoop configuration in order to parse XML
      * Tried Data bricks but many null values are occuring
      * Switched to XMLInoutFormat from previous project
      **/
//    LOG.info("Setting up Spark configuration parameters")
    val hadoopConfiguration = new Configuration()
    hadoopConfiguration.set("xmlinput.start", "<article>|<inproceedings>")
    hadoopConfiguration.set("xmlinput.end", "</article>|</inproceedings>")
    hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", args(0));



    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark XML")
      .config("spark.master", "local")
      .getOrCreate()
    val jSparkContext = new JavaSparkContext(sparkSession.sparkContext)
//    val sSparkContext = new SparkContext(sparkConfiguration)
    val tag = jSparkContext.newAPIHadoopRDD(hadoopConfiguration, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])


    /**
      * This mapper takes the rdd and returns the professor connections
      * in a tuple and flattens it
      * Then gets distinct keys snd mspd corresponding vaues
      **/
  LOG.info("Creating RDD and Mapping")
    val professor_map = tag.rdd.flatMap{ s =>
//      val document = StringEscapeUtils.unescapeHtml4(s._2.toString)       // Uncomment if you want to remove unnecessary accesnts/ditrics
      val document = s._2.toString
      val myPair = getProfessor(document)
      myPair.toList
    }
      .distinct().groupByKey().cache()


  /**
    * Calling getRank() method which iterative computes the rank
    **/
  val sorted_descending = getRanks(professor_map)

//  output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
  LOG.info("Saving ranks to a file...")
  sorted_descending.saveAsTextFile(args(1))              //  Saving output into the folder specified
  LOG.info("All done.")
//    println("Reached End of Code!")       // Printing end of main
  LOG.info("Execution Finished!")
//  }
}
