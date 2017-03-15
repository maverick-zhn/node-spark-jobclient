/*
 ________                                                 _______   _______
/        |                                               /       \ /       \
$$$$$$$$/______   __    __   ______   _______    ______  $$$$$$$  |$$$$$$$  |
   $$ | /      \ /  |  /  | /      \ /       \  /      \ $$ |  $$ |$$ |__$$ |
   $$ |/$$$$$$  |$$ |  $$ |/$$$$$$  |$$$$$$$  |/$$$$$$  |$$ |  $$ |$$    $$<
   $$ |$$ |  $$/ $$ |  $$ |$$    $$ |$$ |  $$ |$$ |  $$ |$$ |  $$ |$$$$$$$  |
   $$ |$$ |      $$ \__$$ |$$$$$$$$/ $$ |  $$ |$$ \__$$ |$$ |__$$ |$$ |__$$ |
   $$ |$$ |      $$    $$/ $$       |$$ |  $$ |$$    $$/ $$    $$/ $$    $$/
   $$/ $$/        $$$$$$/   $$$$$$$/ $$/   $$/  $$$$$$/  $$$$$$$/  $$$$$$$/
 */

/**      In God we trust
  * Created by: Servio Palacios on 2016.05.26.
  * Source: ConnectedComponents.scala
  * Author: Servio Palacios
  * Last edited: 2017.03.09 13:55
  * Description: Spark Job Connector using REST API
  */

/* Package related to the Job Server */
package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

/* spark references */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/* GraphX references */
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

/* ES references */
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark


object ConnectedComponentsES extends SparkJob {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
      .setMaster("local[4]")
      .setAppName("ConnectedComponentsES")

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

    Try(config.getString("comp.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No comp.string config param"))

    Try(config.getString("persisted.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No persisted.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    /* Received Parameters */
    val schema = config.getString("schema.string")
    val strVerticesTable = config.getString("vertices.string")
    val strEdgesTable = config.getString("edges.string")
    val strVertexId = config.getString("vertexId.string")
    val strSource = config.getString("source.string")
    val strTarget = config.getString("target.string")
    val strComp = config.getString("comp.string")
    val strPersisted = config.getString("persisted.string")

    /* Read vertices from ES Benchmark */
    val t0 = System.nanoTime()

    /* Get vertices from index and stored as rdd */
//: VertexRDD[String]
    val verticesRDD = sc.esRDD("biogrid/v")
    //val verticesRDD = sc.esRDD("biogrid/v", Map[String,String]("es.read.field.include"->"id"))

    val vertexRDD: RDD[(VertexId,Long)] = verticesRDD.map(
         x=> (
           x._2.get("id").get.asInstanceOf[Long],
           x._2.get("id").get.asInstanceOf[Long]
         )
    )

    val vertexSet = VertexRDD(vertexRDD)

    /* Read vertices from ES Benchmark */
    val t1 = System.nanoTime()


    //val rowsCassandra: RDD[CassandraRow] = sc.cassandraTable(schema, strEdgesTable)
    //  .select(strSource, strTarget)

    /* Convert RDD into edgeRDD */
    /*val edgesRDD: RDD[Edge[Int]] = rowsCassandra.map(x =>
      Edge(
        x.getLong(strSource),
        x.getLong(strTarget)
      ))
    */

    //val vertexSet = VertexRDD(vertexRDD1)

     /* Read edges from ES Benchmark */
    val t2 = System.nanoTime()

    val edgesESRDD = sc.esRDD("biogrid/e")
    edgesESRDD.count()

    val t3 = System.nanoTime()

    val edgesRDD: RDD[Edge[Long]] = edgesESRDD.map(
      x=> (
        Edge(
        x._2.get("source").get.asInstanceOf[Long],
        x._2.get("target").get.asInstanceOf[Long]
        )
      )
    )

    /* Build the initial Graph */
    val graph = Graph(vertexSet, edgesRDD)

    /* Benchmark ConnectedComponents computation time */
    //val t2 = System.nanoTime()

    /* Find the connected components */
    val cc = graph.connectedComponents().vertices

    /* End Benchmark ConnectedComponents computation time */
    //val t3 = System.nanoTime()

    println("Loading Vertices from Time: " + (t1-t0) + "ns")
    println("Loading Edges from ES time: " + (t3-t2) + "ns")

    /* Collect result */
    //cc.collect()

  }//runJob

}//ConnectedComponentsES



