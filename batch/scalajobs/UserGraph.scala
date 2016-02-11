import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import com.twitter.jsr166e.LongAdder

object GitUserGraph {
  def main(args: Array[String]) {

    val path = "hdfs://ec2-52-89-26-208.us-west-2.compute.amazonaws.com:9000"
    val filepath = "/data/github//"
    val vertexFile = path + filepath + "/vertexFile/*" // git users vertex
    val edgeFile = path + filepath + "/edgeFile/*" // edges

    val conf = new SparkConf()
      .setMaster("spark://ip-172-31-0-68:7077")
      .setAppName("GitUserGraph")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.executor.memory", "6g")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val userGraph = GraphLoader.edgeListFile(sc, edgeFile)

    //Degree of Graph  val degrees
    val degrees: VertexRDD[Int] = userGraph.degrees.cache()
    val stat = degrees.map(_._2).stats()
   
   //Page Ranks
    val ranks = userGraph.pageRank(0.0001).vertices
    val sortedRanks = ranks.map(_.swap).sortByKey(false)

    //  calculating connected Components
    val ccGraph = userGraph.connectedComponents() //: Graph[VertexId, Long]
    val componentCounts = sortedConnectedComponents(ccGraph, "ccFile")
    println("component size: " + componentCounts.size)

    componentCounts.take(10).foreach(println)

   val componentLists = HashMap[VertexId, ListBuffer[VertexId]]()
   ccGraph.vertices.collect().foreach { id =>
     if (!componentLists.contains(id._2)) {
       componentLists(id._2) = new ListBuffer[VertexId]
     }
     componentLists(id._2) += id._1
   }

    componentLists.saveToCassandra("community", "adj_list", SomeColumns("vertex_id", "adjList"))
  }

//sorted connected components
  def sortedConnectedComponents(connectedComponents: Graph[VertexId, _], filePath: String)
  : Seq[(VertexId, Long)] = {
    val componentCounts = connectedComponents.vertices.map(_._2).countByValue //map of vertexid -> count cc
    val sortedCc = componentCounts.toSeq.sortBy(_._2).reverse
    componentCounts.take(10).foreach(println) //(876778,14) (119860,8)
    sortedCc
  }
}