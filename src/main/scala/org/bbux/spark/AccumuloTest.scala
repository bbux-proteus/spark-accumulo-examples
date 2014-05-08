package org.bbux.spark

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.conf.Configuration

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat

object AccumuloTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "HBaseTest",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val conf = new Configuration()
    InputFormatBase.setInputInfo(conf, "root", "s3cret".getBytes(), "grades", null)
    InputFormatBase.setZooKeeperInstance(conf, "pixydust", "localhost")

    val accumuloRDD = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], 
      classOf[org.apache.accumulo.core.data.Key],
      classOf[org.apache.accumulo.core.data.Value])

    accumuloRDD.count()

    System.exit(0)
  }
}
