import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFiles

object Vasanthi_Mudunuri_Program_2 {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("Movie leastRatings").setMaster("local")
    val sc=new SparkContext(conf)
    val inputpath1="hdfs://hadoop1:9000"+args(0)
    val inputpath2="hdfs://hadoop1:9000"+args(1)
    val outputpath="hdfs://hadoop1:9000"+args(2)
    sc.addFile(inputpath1) //adding file to distributed cache
    val lines: RDD[String] = sc.textFile(SparkFiles.get("movies.dat"))  //retrieving file
    val movieID =lines.map(x =>(x.split("::")(0).toInt,x.split("::")(1)))
    val lines1: RDD[String] = sc.textFile(inputpath2)
    val ratings=lines1.map(x=>(x.split("::")(1).toInt,x.split("::")(2).toInt))
    val minratings=ratings.reduceByKey(math.min(_,_)) //finding minimum ratings
    val joindata: RDD[(String,String)]=movieID.join(minratings).map{(x=>(x._2._1.toString(),x._2._2.toString()))}
    val sorteddata=joindata.sortByKey(false) //sorting in descending order
    sorteddata.saveAsTextFile("hdfs://hadoop1:9000"+args(2))
  }
}
