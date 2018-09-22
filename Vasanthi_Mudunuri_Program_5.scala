import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Vasanthi_Mudunuri_Program_5 {
  def FirstAlphabetIndex(input: String): String = { //To check index
    if(input.toUpperCase().equals('A') || input.toUpperCase().equals('C') || input.toUpperCase().equals('E') ||
        input.toUpperCase().equals('G') || input.toUpperCase().equals('I') || input.toUpperCase().equals('K') ||
        input.toUpperCase().equals('M') || input.toUpperCase().equals('O') || input.toUpperCase().equals('Q') || 
        input.toUpperCase().equals('S') || input.toUpperCase().equals('U') || input.toUpperCase().equals('W') ||
        input.toUpperCase().equals('Y')){
      return "Odd Index"+":"+input
    }else
      return "Even Index"+":"+input
  }
  def Length(input: String): String = { //To get length of word
    val oddLength ="Odd length"
    val evenLength = "Even Length"
    if (input.length%2 == 0) {
    return evenLength+":"+input.length().toString()
    }
    else 
    return oddLength+":"+input.length().toString()
  }
  def main(args: Array[String]) {
  val conf=new SparkConf().setAppName("Spark Streaming").setMaster("local")
  val streamingcontext=new  StreamingContext(conf,Seconds(4))
  val outputpath="hdfs://hadoop1:9000"+args(1)
  val lines=streamingcontext.socketTextStream("localhost",args(0).toInt)
  val FirstAlphabetIndexandLength=lines.map { x => (FirstAlphabetIndex(x),Length(x))}
  FirstAlphabetIndexandLength.print()
  FirstAlphabetIndexandLength.saveAsTextFiles(outputpath) //saving to file
  streamingcontext.start()
  streamingcontext.awaitTermination()
  }
}
