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
  * Created by: Servio Palacios on 2016.09.08.
  * Source: personalizedPR.scala
  * Author: Servio Palacios
  * Description: Spark Job Connector using REST API
  */


/**
  * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
  * PageRank and edge attributes containing the normalized edge weight.
  *
  * @param graph the graph on which to compute PageRank
  * @param tol the tolerance allowed at convergence (smaller => more accurate).
  * @param resetProb the random reset probability (alpha)
  *
  * @return the graph containing with each vertex containing the PageRank and each edge
  *         containing the normalized weight.
  */

/* Package related to the Job Server */
package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

/* spark references */
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/* GraphX references */
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD


object PR extends SparkJob {

  def main(args: Array[String]) {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
      .setMaster("local[4]")
      .setAppName("personalizedPR")

    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  /* Validate incoming parameters */
  /* In here I use schemas to determine in which Graph I will run the algorithms */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))

    Try(config.getString("alpha.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No alpha.string config param"))

    Try(config.getString("TOL.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No TOL.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val schema = config.getString("input.string")

    val g = GraphLoader.edgeListFile(sc, "cit-Hepth.txt")

    g.personalizedPageRank(9207016, 0.001)
      .vertices
        .filter(_._1 != 9207016)
        .reduce((a,b) => if (a._2 > b._2) a else b)


  }//runJob

}//PR object


