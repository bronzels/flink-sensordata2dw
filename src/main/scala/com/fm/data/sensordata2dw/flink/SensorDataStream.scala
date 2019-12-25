package com.fm.data.sensordata2dw.flink

import java.util.UUID

import at.bronzels.libcdcdw.kudu.myenum.KuduWriteEnum
import at.bronzels.libcdcdw.kudu.myenum.KuduWriteEnum._
import at.bronzels.libcdcdw.conf.{KuduTableEnvConf, DistLockConf}
import at.bronzels.libcdcdwstr.bean.SourceRecordKafkaJsonNode
import at.bronzels.libcdcdwstr.flink.util
import at.bronzels.libcdcdwstr.flink.util.MyJackson
import at.bronzels.libcdcdwstr.flink.sink.JsonNode2KuduMap
import com.fm.data.sensordata2dw.MyName._
import com.fm.data.sensordata2dw.sensordata.MySensorData
import com.fm.data.sensordata2dw.sensordata.MySensorData._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{JsonNodeFactory, JsonNodeType, ObjectNode}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.control.Breaks._

object SensorDataStream {

  def getStreamJsonedFilteredFlattendedSplitted(inputStream: DataStream[SourceRecordKafkaJsonNode], projectId: Long): Tuple2[DataStream[JsonNode], DataStream[JsonNode]] = {
    val jsonedFilteredFlattendedStream = inputStream
      .filter(record => record.getData.isObject)
      .flatMap((record, out: Collector[JsonNode]) => {
        breakable {
          val node = record.getData.deepCopy().asInstanceOf[ObjectNode]

          if (node == null
            || !node.isObject
          ) {
            break
          }

          if (
            !MyJackson.isExistedValueNode(node, MySensorData.Distinct_id_field_name)
              ||
              !MyJackson.isExistedValueNode(node, MySensorData.Type_field_name)
              ||
              !MyJackson.isExistedValueNode(node, MySensorData.Time_field_name)
              ||
              !MyJackson.isExistedValueNode(node, MySensorData.Project_id_field_name)
              ||
              !MyJackson.isExistedTypeAlliedNode(node, MySensorData.Properties_field_name, JsonNodeType.OBJECT)
          ) {
            break
          }

          val projectValue = util.MyKuduTypeValue.getLong(node, MySensorData.Project_id_field_name)
          //logwarn
          if (projectValue == null) {
            break
          }
          //nologwarn
          if (!projectValue.equals(projectId))
            break
          val distinct_idValue = util.MyKuduTypeValue.getString(node, MySensorData.Distinct_id_field_name)
          if (distinct_idValue != null)
            node.put(MySensorData.Distinct_id_field_name, distinct_idValue)
          else
            break
          val typeValue = util.MyKuduTypeValue.getString(node, MySensorData.Type_field_name)
          if (typeValue != null)
            node.put(MySensorData.Type_field_name, typeValue)
          else
            break
          /*
          val timeValue = util.MyKuduTypeValue.getTimestamp(node, MySensorData.Time_field_name)
          if (timeValue != null)
            node.put(MySensorData.Time_field_name, timeValue)
          else
            break
           */

          import scala.collection.JavaConverters._
          val origObjNode = MyJackson.getRemoved(node, Useless_from_beginning_exclude_field_Arr.toList.asJava).asInstanceOf[ObjectNode]

          val flattenedNode = JsonNodeFactory.instance.objectNode()
          val javaIteratorFields = origObjNode.fields()
          while (javaIteratorFields.hasNext) {
            val entry = javaIteratorFields.next()
            val entryKey = entry.getKey
            val entryValue = entry.getValue
            if (entryValue.isObject && entryKey.equals(MySensorData.Properties_field_name)) {
              val nestedObjNode = entryValue.asInstanceOf[ObjectNode]
              val nestedJavaIteratorFields = nestedObjNode.fields()
              while (nestedJavaIteratorFields.hasNext) {
                val nestedEntry = nestedJavaIteratorFields.next()
                flattenedNode.put(nestedEntry.getKey, nestedEntry.getValue)
              }
            }
            else flattenedNode.put(entryKey, entryValue)
          }
          flattenedNode.put(MyName_mystr_interkafka_ptoffset, record.getGuid)

          val retNode = flattenedNode.asInstanceOf[JsonNode]
          out.collect(retNode)
        }
      })

    //jsonedFilteredFlattendedStream.map(MyJackson.getString(_)).print()

    val outputTag = OutputTag[JsonNode]("usersAsSideOutput")
    val eventsAsMainStream = jsonedFilteredFlattendedStream
      .process(
        new ProcessFunction[JsonNode, JsonNode] {
          override def processElement(
                                       node: JsonNode,
                                       ctx: ProcessFunction[JsonNode, JsonNode]#Context,
                                       out: Collector[JsonNode]): Unit = {
            breakable {
              val typeValue = node.get(Type_field_name).asText
              if (MySensorData.isEventSpecific(typeValue)) {
                if (!MyJackson.isExistedValueNode(node, MySensorData.Event_field_name)) {
                  break
                }
                if (typeValue.equals(MySensorData.Type_value_events_track_signup)) {
                  ctx.output(outputTag, node)
                }
                val objNode = node.asInstanceOf[ObjectNode]
                objNode.put(MyName_mystr_uuid, UUID.randomUUID().toString)
                //objNode.put(MyName.MyName_mystr_dollarkafka_offset, )
                val nodeWithUUID = objNode.asInstanceOf[JsonNode]
                // 将数据发送到常规输出中
                out.collect(nodeWithUUID)
              } else if (MySensorData.isUserSpecific(typeValue)) {
                // 将数据发送到侧输出中
                ctx.output(outputTag, node)
              } else {
                //item is not supported yet
              }
            }
          }
        })
    val usersAsSideOutput: DataStream[JsonNode] = eventsAsMainStream.getSideOutput(outputTag)
    new Tuple2[DataStream[JsonNode], DataStream[JsonNode]](eventsAsMainStream, usersAsSideOutput)
  }

  def getEventsLogedinPutSinkedTupleStream(inputStream: DataStream[JsonNode], eventsKuduTableEnvConf: KuduTableEnvConf, usersKuduTableEnvConf: KuduTableEnvConf, distLockConf: DistLockConf): (DataStream[String], DataStream[String]) = {
    val outputTag = OutputTag[JsonNode]("logedinBulkUpdateAsSideOutput")

    val eventFilteredSplitedMainStream = inputStream
      .filter(node => MyJackson.isExistedValueNode(node, Event_field_name))
      .process(
        new ProcessFunction[JsonNode, JsonNode] {
          override def processElement(
                                       node: JsonNode,
                                       ctx: ProcessFunction[JsonNode, JsonNode]#Context,
                                       out: Collector[JsonNode]): Unit = {
            breakable {
              val typeValue = node.get(Type_field_name).asText
              if (typeValue.equals(Type_value_events_track_signup)) {
                // 将数据发送到侧输出中
                ctx.output(outputTag, node)
              }
              out.collect(node)
            }
          }
        })

    val logedinBulkUpdateFlatMap = new SensorDataEventsTrackSignupBulkJsonNode2KuduMap(eventsKuduTableEnvConf, distLockConf)
    val logedinBulkUpdateStream = eventFilteredSplitedMainStream.getSideOutput(outputTag)
      .flatMap(logedinBulkUpdateFlatMap)

    val eventsUsers2IdsPutFlatMap = new SensorDataEventsMainUsers2IdsPutJsonNode2KuduMap(usersKuduTableEnvConf, distLockConf)
    val sinkFlatMap = new JsonNode2KuduMap(eventsKuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase)

    /*
    eventFilteredSplitedMainStream
      .map(MyJackson.getString(_))
      .print()
     */

    import scala.collection.JavaConverters._
    val bfSinkedStream = eventFilteredSplitedMainStream
      .flatMap(eventsUsers2IdsPutFlatMap)
      .map(node => {
        (Put, MyJackson.getRemoved(node, Events_at_last_exclude_field_Arr.toList.asJava))
      })
    /*
    bfSinkedStream
      .map(tuple => "1:" + tuple._1 + ", 2:" + MyJackson.getString(tuple._2))
      .print()
     */
    val sinkedStream = bfSinkedStream
      .flatMap(sinkFlatMap)

    (logedinBulkUpdateStream, sinkedStream)
  }

  def getUsersSinkedStream(inputStream: DataStream[JsonNode], kuduTableEnvConf: KuduTableEnvConf, distLockConf: DistLockConf): DataStream[String] = {
    val usersIdAddedFieldsCleanedFlatMap = new SensorDataUsersIdAddedJsonFieldsCleanedNode2KuduMap(kuduTableEnvConf, distLockConf)
    val usersIdAddedFieldsCleanedStream = inputStream
      .flatMap(usersIdAddedFieldsCleanedFlatMap)

    val writeEnumAddedStream = usersIdAddedFieldsCleanedStream
      .flatMap((node, out: Collector[(KuduWriteEnum, JsonNode)]) => {
        breakable {
          val typeName = util.MyKuduTypeValue.getString(node, Type_field_name)
          val writeEnum = typeName match {
            case Type_value_users_profile_set => Put
            case Type_value_users_profile_set_once => SetOnInsert4PropAvailable
            case Type_value_users_profile_increment => IncrEmulated
            case Type_value_users_profile_delete => Delete
            case Type_value_users_profile_append => Put
            case Type_value_users_profile_unset => Unset
            case _ => break
          }
          if(writeEnum.equals(Delete)) {
            val nodeOnlyKeepIdIfDelete = JsonNodeFactory.instance.objectNode()
            nodeOnlyKeepIdIfDelete.put(New_users_id_field_name, node.get(New_users_id_field_name).asText())
            (writeEnum, nodeOnlyKeepIdIfDelete)
          } else {
            import scala.collection.JavaConverters._
            val nodeCleaned = MyJackson.getRemoved(node, Users_at_last_exclude_field_Arr.toList.asJava)
            (writeEnum, nodeCleaned)
          }

        }
      })

    val sinkFlatMap = new JsonNode2KuduMap(kuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase)
    val sinkedStream = writeEnumAddedStream
      .flatMap(sinkFlatMap)

    sinkedStream
  }

}
