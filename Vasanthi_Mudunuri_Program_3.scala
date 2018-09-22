import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Vasanthi_Mudunuri_Program_3 {
  def main(args: Array[String]) {
  val spark = SparkSession.builder().master("local").appName("").getOrCreate()
  val ACSfilepath="hdfs://hadoop1:9000"+args(0)
	val CFSfilepath="hdfs://hadoop1:9000"+args(1)
	val outputpath="hdfs://hadoop1:9000"+args(2)
  val readACS=spark.read.csv(ACSfilepath) //reading csv file
	val ACSDataFrame=readACS.select("_c2", "_c3").withColumnRenamed("_c2","State").withColumnRenamed("_c3","TotalPopulation")
  ACSDataFrame.createOrReplaceTempView("StatePopulation")
  val readCFS=spark.read.csv(CFSfilepath) //reading csv file
  val CFSDataFrame=readCFS.select("_c2", "_c8").withColumnRenamed("_c2","State").withColumnRenamed("_c8","Tons")
  CFSDataFrame.createOrReplaceTempView("StateTons")
  val sumoftons = spark.sql("SELECT State,sum(Tons) AS TotalTons FROM StateTons WHERE Tons REGEXP '[0-9]+' GROUP BY State") //calculating sum of tons
	sumoftons.createOrReplaceTempView("StateTotalTons")
  val result = spark.sql("SELECT S.State,S.TotalPopulation,T.TotalTons FROM StatePopulation S LEFT JOIN StateTotalTons T WHERE S.State=T.State").rdd
  val FinalResult: RDD[(String,String)]=result.map( row => {
    val state = row.getString(0)
    val State=s"{State : $state}"
    val TotalPopulation= row.getString(1)
    val TotalTons = row.getDouble(2)
    val PopulationandTons = s"{TotalPopulation: $TotalPopulation, TotalTons: $TotalTons}"
    (State,PopulationandTons)
  })
  FinalResult.saveAsSequenceFile(outputpath,Some(classOf[org.apache.hadoop.io.compress.BZip2Codec])) //saving as sequencefile with compression
 }
}
