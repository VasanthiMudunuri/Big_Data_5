import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFiles

object Vasanthi_Mudunuri_Program_1 {
  def main(args: Array[String]) {
  val conf=new SparkConf().setAppName("AverageMovies perGenre").setMaster("local")
  val sc=new SparkContext(conf)
  val inputpath="hdfs://hadoop1:9000"+args(0)
  val outputpath="hdfs://hadoop1:9000"+args(3)
  val RScriptpath=args(1)
  sc.addFile(args(1))
  val Rfile="Average.r"
  val givengenre=args(2)
  val lines: RDD[String] = sc.textFile(inputpath)
  val movieIDandGenres=lines.map{x =>(x.split("::")(0).toInt,x.split("::")(2))}
  val TotalMoviesCount=movieIDandGenres.count().toInt
  val genres=movieIDandGenres.flatMap(_._2.split("\\|")).filter { x => x == givengenre } //filtering based on given genre
  val GivenGenreCount=genres.count().toInt
  val counts=s"$GivenGenreCount,$TotalMoviesCount"
  val Average=sc.parallelize(List(counts)).pipe(Seq(SparkFiles.get(Rfile),",")) //sending data to R via pipe
  Average.saveAsTextFile(outputpath)
  }
}
