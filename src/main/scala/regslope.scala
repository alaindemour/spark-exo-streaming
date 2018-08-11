
package metrics

import java.time._


import org.apache.hadoop.metrics2.annotation.Metrics
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class RegSlope extends UserDefinedAggregateFunction {
  // Input Data Type Schema

  val t0 = LocalDate.of(2000,1,1)

  def xtime(y : Long ,m : Long): Long = {
    val t = LocalDate.of(y.toInt ,m.toInt ,1)
    val timecoord = t0.until(t)
    timecoord.toTotalMonths
  }


  override def inputSchema: StructType = StructType(
    Array(
      StructField("y", LongType, true),
      StructField("m", LongType, true) ,
      StructField("p", LongType, true)
    ))

  override def bufferSchema : StructType =
    StructType(
      Array(
        StructField("sumx", DoubleType) ,
        StructField("sumy", DoubleType),
        StructField("sumxy", DoubleType),
        StructField("sumx2", DoubleType)
      ))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  //
  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Double](0) + 1
    val y = input.getAs[Long](0)
    val m = input.getAs[Long](1)
    val p = input.getAs[Long](2)
    buffer(1) = buffer.getAs[Double](1) + y
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {

    val fixedClock = Clock.fixed(Instant.ofEpochSecond(1234567890L), ZoneOffset.ofHours(0))

    var i = 0L

    buffer.getDouble(1)
  }

}

//sqlContext.udf.register("slope", new RegSlope)