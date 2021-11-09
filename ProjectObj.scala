import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object ProjectObj extends App {

  val conf = new SparkConf().setAppName("Project1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark: SparkSession = SparkSession.builder()
    .getOrCreate()

  //val normalFile = sc.textFile(path = "/Users/chprasad/Desktop/chaitanya personal/study/tutorials/practice_project/Project1/src/main/resources/*")
 // normalFile.map(x => (NullWritable.get(),x)).saveAsSequenceFile(path = "/Users/chprasad/Desktop/chaitanya personal/study/tutorials/practice_project/Project1/src/main/output/")
//val columns = Seq("key","val")
  import spark.sqlContext.implicits._
  val tmp = sc.sequenceFile(path = "/Users/chprasad/Desktop/chaitanya personal/study/tutorials/practice_project/Project1/src/main/output/", classOf[NullWritable], classOf[Text]).toDF("col1")
  tmp.show()


}

