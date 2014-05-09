package org.bbux.spark;

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat

object AccumuloSparkPageRank {
  def main(args: Array[String]) {
  
    if (args.length < 7) {
      System.err.println("Usage: AccumuloTest <master> <user> <password> <table> <instance> <zookeepers> <iterations>");
      System.exit(1);
    }

    val sc = new SparkContext(args(0), "AccumuloTest",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val conf = new Configuration()
    InputFormatBase.setInputInfo(conf,
       args(1),			//user
       args(2).getBytes(),	//password
       args(3), 		//table
       null)			//auths
    
    InputFormatBase.setZooKeeperInstance(conf,
       args(4),	//instance
       args(5)) //zookeepers

    var iters = args(6).toInt //number of iterations to run

    val accumuloRDD = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], 
      classOf[org.apache.accumulo.core.data.Key],
      classOf[org.apache.accumulo.core.data.Value])

    // key is url, col fam is linked to url, qualifier and value ignored
    val links  = accumuloRDD.map { tuple =>
       val key = tuple._1
       val value = tuple._2
       val outKey = key.getRow().toString()
       val outVal = key.getColumnFamily().toString()
       (outKey, outVal)
    }.distinct().groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    sc.stop()
  }
}
