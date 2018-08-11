
package metrics

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
  override def inputSchema: StructType = StructType(
    Array(
      StructField("x", LongType, true),
      StructField("y", LongType, true)
    ))

  override def bufferSchema : StructType =
    StructType(
      Array(
        StructField("count", LongType) ,
        StructField("sum", DoubleType)))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  //
  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) + input.getAs[Long](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(1)
  }

}

//sqlContext.udf.register("slope", new RegSlope)