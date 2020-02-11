/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2019 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.catalyst

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object JavaConverter {

    def convertRowValue(row: Row, index: Int, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => row.getList(index)
            case MapType(keyType, valueType, _) => row.getMap(index)
            case StructType(fields) => convertStruct(row.getStruct(index), fields)
            case StringType => row.getString(index)
            case _ => row.get(index)
        }
    }

    def convertArray(array: ArrayData, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => array.toArray[ArrayData](elementType).map(convertArray(_, innerType)).toSeq.asJava
            case MapType(keyType, valueType, _) => array.toArray[MapData](elementType).map(convertMap(_, keyType, valueType)).toSeq.asJava
            case structType: StructType => array.toArray[Row](structType).map(convertStruct(_, structType.fields)).toSeq.asJava
            case StringType => convertStringArray(array).asJava
            case _ => array.toArray[Any](elementType).toSeq.asJava
        }
    }

    def convertMap(map: MapData, keyType: DataType, valueType: DataType): util.Map[String, Any] = {
        if (keyType != StringType) throw new IllegalArgumentException(
            s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
        val keys = convertStringArray(map.keyArray())
        val values = valueType match {
            case ArrayType(innerType, _) => map.valueArray().toArray[ArrayData](valueType).map(convertArray(_, innerType)).toSeq
            case MapType(innerKeyType, innerValueType, _) => map.valueArray().toArray[MapData](valueType).map(convertMap(_, innerKeyType, innerValueType)).toSeq
            case structType: StructType => map.valueArray().toArray[Row](structType).map(convertStruct(_, structType.fields)).toSeq
            case StringType => convertStringArray(map.valueArray())
            case _ => map.valueArray().toArray[Any](valueType).toSeq
        }
        val kvPairs = for (i <- 0 until map.numElements()) yield keys(i) -> values(i)
        Map(kvPairs: _*).asJava
    }

    def convertStruct(row: Row, fields: Seq[StructField]): util.Map[String, Any] = {
        val kvPairs = for (i <- 0 until row.length) yield
            if (row.isNullAt(i)) fields(i).name -> null
            else fields(i).name -> convertRowValue(row, i, fields(i).dataType)
        Map(kvPairs: _*).asJava
    }

    def convertStringArray(array: ArrayData): Seq[String] =
        array.toArray[UTF8String](StringType).toSeq.map(_.toString)

}
