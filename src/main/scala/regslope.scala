import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, LongType, ArrayType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{ListBuffer,ArrayBuffer}


class RegSlope extends UserDefinedAggregateFunction {
  // Input Data Type Schema
  override def inputSchema: StructType = StructType(

    Array(
      StructField("artist",StringType,true),
      StructField("artistDisplayName",StringType,true),
      StructField("auctionHouse",StringType,true),
      StructField("birth",LongType,true),
      StructField("brand",StringType,true),
      StructField("cid",StringType,true),
      StructField("crecre", StructType(Array(StructField("name",StringType,true), StructField("parent",StringType,true))),true),
      StructField("currency",StringType,true),
      StructField("death",LongType,true),
      StructField("discoveryCrawlId",StringType,true),
      StructField("edition",LongType,true),
      StructField("editionOf",LongType,true),
      StructField("estimated",StringType,true),
      StructField("high",LongType,true),
      StructField("img",StringType,true),
      StructField("latestCrawlId",StringType,true),
      StructField("lot",LongType,true),
      StructField("low", LongType,true),
      StructField("month",LongType, true),
      StructField("name" , StringType,true),
      StructField("real", LongType, true),
      StructField("realized", StringType, true),
      StructField("sale", LongType, true),
      StructField("salesTitle", StringType,true),
      StructField("state", StringType, true),
      StructField("stuff", StringType,true),
      StructField("url", StringType, true),
      StructField("x", FloatType, true),
      StructField("y", FloatType, true),
      StructField("year", LongType, true),
      StructField("z", FloatType, true)
    ))

  override def bufferSchema : StructType =
    StructType(
      Array(
        StructField("count", LongType) ,
        StructField("product", DoubleType)))

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
    buffer(1) = buffer.getAs[Double](1) + input.getAs[Long]("real")
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble)
  }

}

//sqlContext.udf.register("slope", new RegSlope)