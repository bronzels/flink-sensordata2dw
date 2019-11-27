package com.fm.data.sensordata2dw.flink

import java.util

import at.bronzels.libcdcdwstr.flink.util.MyKuduTypeValue
import at.bronzels.libcdcdw.conf.{DistLockConf, KuduTableEnvConf}
import at.bronzels.libcdcdwstr.flink.sink.Sink2KuduCommonMap
import com.fm.data.sensordata2dw.sensordata.MySensorData
import com.fm.data.sensordata2dw.sensordata.MySensorData.{Distinct_id_field_name, Event_field_name, Event_track_signup_original_id_field_name, New_common_user_id_field_name, Type_field_name, Type_value_events_track_signup}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

import scala.util.control.Breaks.{break, breakable}

class SensorDataEventsTrackSignupBulkJsonNode2KuduMap(override val kuduTableEnvConf: KuduTableEnvConf, override val distLockConf: DistLockConf) extends Sink2KuduCommonMap[JsonNode, String](kuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase) {

  override def flatMap(node: JsonNode, out: Collector[String]) = {
    val tuple = getKuduAndTsIndex
    val myKudu = tuple._1
    val _dwsynctsKuduFieldIndex = tuple._2

    breakable {
      val distinct_id = node.get(Distinct_id_field_name).asText()
      val original_id = MyKuduTypeValue.getString(node, Event_track_signup_original_id_field_name)
      val user_id = MyKuduTypeValue.getDouble(node, New_common_user_id_field_name)
      if (original_id == null || user_id == null)
        break
      val conditionMap = new util.HashMap[String, Object]()
      conditionMap.put(Distinct_id_field_name, original_id)
      val updateMap = new util.HashMap[String, Object]()
      updateMap.put(New_common_user_id_field_name, user_id)
      val ret = myKudu.updateAfterSelected(true, conditionMap, updateMap, at.bronzels.libcdcdw.Constants.tsFieldIndexReusedDwsync, isSrcFieldNameWTUpperCase)
      out.collect(ret)
    }
  }
}
