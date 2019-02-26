package gov.nih.nlm.ncbi.brdd


import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag


class BlastRDD[T: ClassTag](@transient private val _sc: SparkContext) extends RDD[T](_sc, Nil) {

  override def compute(raw: Partition, context: TaskContext): Iterator[T] = ???
  override protected def getPartitions: Array[Partition] = ???
}

object BlastRDD {
  implicit class SparkContextOps(sc: SparkContext) {

    def blast[T: ClassTag](): BlastRDD[T] = new BlastRDD[T](sc)

  }
}
