package org.bbux.spark;

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.iterators.user.RegExFilter;

object AccumuloMetadataCount {
  def main(args: Array[String]) {
  
    if (args.length < 7) {
      System.err.println("Usage: AccumuloTest <master> <user> <password> <table> <instance> <zookeepers> <row regex>");
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

    val is = new IteratorSetting(1, classOf[RegExFilter]);
    RegExFilter.setRegexs(is, null, null, args(6), null, false); 
    InputFormatBase.addIterator(conf, is)

    val accumuloRDD = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], 
      classOf[org.apache.accumulo.core.data.Key],
      classOf[org.apache.accumulo.core.data.Value])

    // row is sequential integer id
    // columns are ones that match supplied regex
    val output  = accumuloRDD.map { tuple =>
       val key = tuple._1
       val value = tuple._2
       val outKey = new String(value.get())
       (outKey, 1)
    }.reduceByKey(_+_)

    output.foreach(tup => println(tup._1 + " -> " + tup._2))

    sc.stop()
  }
}
