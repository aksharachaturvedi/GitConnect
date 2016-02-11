import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkContext,SparkConf}

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Created by Akshara on 1/21/16.
  */

import org.apache.spark.{SparkConf, SparkContext}

object GitFollwersGraph {
  def main(args: Array[String]) {

    val path = "hdfs://ec2-52-89-26-208.us-west-2.compute.amazonaws.com:9000"
    val filepath = "/data/github/txt/"
    val vertexFile = path + filepath + "users/*.csv" // Dump of git users information
    val edgeFile = path + filepath + "followers/*.csv" // Dump of followers information
    val master_ip = "";

    val conf = new SparkConf()
      .setMaster("spark://"+master_ip+":7077")
      .setAppName("GitFollowersGraph")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.executor.memory", "6g")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Load my user data and parse into tuples of user id and attribute list
    val usersVertex = (sc.textFile(vertexFile)
      .map(line => line.split(" "))
      .map(parts => (parts.head, parts.tail)))

    val user: RDD[String] = sc.textFile(vertexFile)
    val vertices = user.map { line => val fields = line.split(' ')
      (fields(0).toLong, fields(1))
    }

    // Parse the edge data which is already in userId -> userId format
    val followerGraph = GraphLoader.edgeListFile(sc, edgeFile)

    val ranks = followerGraph.pageRank(0.0001).vertices

    val ranksByUsername = vertices.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    //get the ranks in desc order
    val ranksOfUsersAsc =  ranksByUsername.join(usersVertex).sortBy(_._2._1, ascending=false).map(_._2._2)
    val ranktop10 = ranksOfUsersAsc.take(10)

    println(ranksOfUsersAsc.collect().mkString("\n"))
    ranksOfUsersAsc.saveToCassandra("community", "followrank", SomeColumns("login", "pagerank"))
  }

  //CURRENT DATE AND TIME 20080725_013755
  def currentDateTime() : String = {
    val date = Calendar.getInstance().getTime();
    val sdf = new SimpleDateFormat("yyyyMMdd_hhmm");
    return sdf.format(date);
  }
}