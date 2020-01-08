package at.bronzels.track2dw.flink

import java.util

import at.bronzels.libcdcdwstr.flink.util.MyKuduTypeValue
import at.bronzels.libcdcdw.conf.{DistLockConf, KuduTableEnvConf}
import at.bronzels.libcdcdw.util.MyLog4j2

import at.bronzels.libcdcdwstr.flink.sink.Sink2KuduCommonMap

import at.bronzels.track2dw.sensordata.MySensorData
import at.bronzels.track2dw.sensordata.MySensorData.{Distinct_id_field_name, Event_field_name, Event_track_signup_original_id_field_name, New_common_user_id_field_name, Type_field_name, Type_value_events_track_signup}

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.message.ObjectMessage

import scala.util.control.Breaks.{break, breakable}

class TrackDataEventsTrackSignupBulkJsonNode2KuduMap(override val kuduTableEnvConf: KuduTableEnvConf, override val distLockConf: DistLockConf) extends Sink2KuduCommonMap[JsonNode, String](kuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase) {
  private val lo4j2LOG = LogManager.getLogger(classOf[Nothing])

  override def flatMap(node: JsonNode, out: Collector[String]) = {
    val tuple = getKuduAndTsIndex
    val myKudu = tuple._1
    val _dwsynctsKuduFieldIndex = tuple._2

    breakable {
      val distinct_id = node.get(Distinct_id_field_name).asText()
      val original_id = MyKuduTypeValue.getString(node, Event_track_signup_original_id_field_name)
      val user_id = MyKuduTypeValue.getLong(node, New_common_user_id_field_name)
      if (original_id == null || user_id == null) {
        if (lo4j2LOG.isWarnEnabled()) {
          val map = new util.HashMap[String, Object]()
          map.put("node", node)
          MyLog4j2.markBfLog(logContext, String.format("track sign up event with null original_id/user_id"));val msg = new ObjectMessage(map);lo4j2LOG.warn(msg)
          MyLog4j2.unmarkAfLog()
        }
        break
      }
      val conditionMap = new util.HashMap[String, Object]()
      conditionMap.put(Distinct_id_field_name, original_id)
      val updateMap = new util.HashMap[String, Object]()
      updateMap.put(New_common_user_id_field_name, user_id)
      val ret = myKudu.updateAfterSelected(true, conditionMap, updateMap, at.bronzels.libcdcdw.Constants.tsFieldIndexReusedDwsync, isSrcFieldNameWTUpperCase)
      out.collect(ret)
    }
  }
}
