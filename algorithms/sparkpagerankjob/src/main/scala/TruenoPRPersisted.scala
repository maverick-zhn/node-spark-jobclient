/*

 ________                                                 _______   _______
/        |                                               /       \ /       \
$$$$$$$$/______   __    __   ______   _______    ______  $$$$$$$  |$$$$$$$  |
   $$ | /      \ / /|  / /| /      \ /       \  /      \ $$ |  $$ |$$ |__$$ |
   $$ |/$$$$$$  |$$ |  $$ |/$$$$$$  |$$$$$$$  |/$$$$$$  |$$ |  $$ |$$    $$<
   $$ |$$ |  $$/ $$ |  $$ |$$    $$ |$$ |  $$ |$$ |  $$ |$$ |  $$ |$$$$$$$  |
   $$ |$$ |      $$ \__$$ |$$$$$$$$/ $$ |  $$ |$$ \__$$ |$$ |__$$ |$$ |__$$ |
   $$ |$$ |      $$    $$/ $$       |$$ |  $$ |$$    $$/ $$    $$/ $$    $$/
   $$/ $$/        $$$$$$/   $$$$$$$/ $$/   $$/  $$$$$$/  $$$$$$$/  $$$$$$$/

 */

/**      In God we trust
  * Created by: Servio Palacios on 2016.09.19.
  * Source: TruenoPRPersist.scala
  * Author: Servio Palacios
  * Description: Spark Job Connector using REST API
  */

/**
  * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
  * PageRank and edge attributes containing the normalized edge weight.
  * Results persisted to Cassandra Backend if indicated.
  *
  *  graph the graph on which to compute PageRank
  *  tol the tolerance allowed at convergence (smaller => more accurate).
  *  resetProb the random reset probability (alpha)
  *
  * @return the graph containing with each vertex containing the PageRank and each edge
  *         containing the normalized weight.
  */

/* Package related to the Job Server */
package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
//import com.google.commom.collect.InmutableMap
import scala.util.Try

/* spark references */
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/* GraphX references */
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD


object TruenoPRPersisted extends SparkJob {

  case class Results(id: String, computed: Map[String, Map[String, UDTValue]])

  def main(args: Array[String]) {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "8003")
      .setMaster("local[4]")
      .setAppName("TruenoPRPersisted")

    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  /* Validate incoming parameters */
  /* In here I use schemas to determine in which Graph I will run the algorithms */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("schema.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No schema.string config param"))

    Try(config.getString("vertices.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No vertices.string config param"))

    Try(config.getString("edges.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No edges.string config param"))

    Try(config.getString("vertexId.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No vertexId.string config param"))

    Try(config.getString("source.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No source.string config param"))

    Try(config.getString("target.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No target.string config param"))

    Try(config.getString("alpha.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No alpha.string config param"))

    Try(config.getString("TOL.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No TOL.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    /* Received Parameters */
    val schema = config.getString("schema.string")
    val strVerticesTable = config.getString("vertices.string")
    val strEdgesTable = config.getString("edges.string")
    val strVertexId = config.getString("vertexId.string")
    val strSource = config.getString("source.string")
    val strTarget = config.getString("target.string")
    val alpha = config.getDouble("alpha.string")
    val TOL = config.getDouble("TOL.string")

    /* Get table from keyspace and stored as rdd */
    val vertexRDD1: RDD[(VertexId, String)] = sc.cassandraTable(schema, strVerticesTable)

    /* Get Cassandra Row and Select id */
    val vertexCassandra: RDD[CassandraRow] = sc.cassandraTable(schema, strVerticesTable)
      .select(strVertexId)

    /* Convert Cassandra Row into Spark's RDD */
    val rowsCassandra: RDD[CassandraRow] = sc.cassandraTable(schema, strEdgesTable)
      .select(strSource, strTarget)

    /* Convert RDD into edgeRDD */
    val edgesRDD: RDD[Edge[Int]] = rowsCassandra.map(x =>
      Edge(
        x.getLong(strSource),
        x.getLong(strTarget)
      ))

    val vertexSet = VertexRDD(vertexRDD1)

    /* Build the initial Graph */
    val graph = Graph(vertexSet, edgesRDD)

    /* Run PageRank until convergence*/
    val ranks = graph.pageRank(TOL).vertices

    ranks.map( x =>
      Results(
        x._1.toString,
        Map("PageRank" -> Map( "result" -> UDTValue.fromMap(
          Map("type" -> "number",
          "value" -> x._2.toString)
           )))
      )

    ).saveToCassandra(schema, strVerticesTable, SomeColumns(strVertexId, "comp"))
    //ranks.collect()

  }//runJob

}//TruenoPRPersisted object



