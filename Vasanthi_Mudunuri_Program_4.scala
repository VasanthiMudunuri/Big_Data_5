import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object Vasanthi_Mudunuri_Program_4 {
  def main(args: Array[String]) {
  val conf=new SparkConf().setAppName("User Tweets").setMaster("local")
  val sc=new SparkContext(conf)
  val inputpath="hdfs://hadoop1:9000"+args(0)
  val outputpath="hdfs://hadoop1:9000"+args(1)
  val file: RDD[String] = sc.textFile(inputpath).cache()
  val vertices=file.map {x =>(x.split(",")(1).split("\\s+")(0))}
  val verticesArray=vertices.collect()
  var users=collection.immutable.Map[Int,String]()
  for(i <- 0 until vertices.count().toInt) //giving id to each user
  {
  users +=(i -> verticesArray(i))
  }
  var Users=users.map{case (key,value) => (key.toLong,value)}
  def keyForValue(map: Map[Long, String], value: String) = { //to get key by value from map
  try{
  val revMap = map map {_.swap}
  val key = revMap(value)
  key
  }catch{
    case e: Exception =>
     Users += (Users.size.toLong -> value)
     (Users.size.toLong)-1
  }
}
  val edges=file.map(x =>(x.split(",")(1).split("\\s+")(0),x.split(",")(1).split("\\s+")(1))).filter{case (key,value)=> value != "N/A"}
  val edgesArray=edges.collect()
  val relation=edges.map{case(v) => Edge(keyForValue(Users,v._1),keyForValue(Users,v._2),"")}
  val nodes=sc.makeRDD(Users.toArray)
  val graph=Graph(nodes,relation) //graph construction
  val UserInfluence=lib.ShortestPaths.run(graph,Array(nodes.keys.id)).vertices  
  UserInfluence.saveAsTextFile(outputpath+"/UserInfluence")
  val PageRank=graph.pageRank(0.0001).vertices
  val UsersPageRank=nodes.join(PageRank).map{case (id,(username,rank)) => (username,rank)}
  UsersPageRank.saveAsTextFile(outputpath+"/UsersPagerank")
  val ConnectedUsers=graph.connectedComponents(10).vertices.map(_.swap).groupByKey.map(_._2)
  ConnectedUsers.saveAsTextFile(outputpath+"/ConnectedUsers")
  val StronglyConnectedUsers=graph.stronglyConnectedComponents(10).vertices.map(_.swap).groupByKey.map(_._2)
  StronglyConnectedUsers.saveAsTextFile(outputpath+"/StronglyConnectedUsers")
  val RecommendUser=graph.personalizedPageRank(513164221,0.001).vertices.filter(_._1!=513164221)
  RecommendUser.saveAsTextFile(outputpath+"/RecommendUser")
  val UserCommunities= lib.LabelPropagation.run(graph,5).vertices
  UserCommunities.saveAsTextFile(outputpath+"/UserCommunities")
  }
}
