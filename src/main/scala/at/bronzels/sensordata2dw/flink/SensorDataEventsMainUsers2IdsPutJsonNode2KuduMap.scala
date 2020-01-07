package at.bronzels.sensordata2dw.flink

import java.util

import at.bronzels.libcdcdwstr.flink.util.MyKuduTypeValue
import at.bronzels.libcdcdw.conf.{DistLockConf, KuduTableEnvConf}
import at.bronzels.libcdcdwstr.flink.sink.Sink2KuduCommonMap
import at.bronzels.sensordata2dw.MyName
import at.bronzels.sensordata2dw.MyName
import at.bronzels.sensordata2dw.sensordata.MySensorData
import at.bronzels.sensordata2dw.sensordata.MySensorData._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

import scala.util.control.Breaks.{break, breakable}
import org.apache.logging.log4j.LogManager

class SensorDataEventsMainUsers2IdsPutJsonNode2KuduMap(override val kuduTableEnvConf: KuduTableEnvConf, override val distLockConf: DistLockConf) extends Sink2KuduCommonMap[JsonNode, JsonNode](kuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase) {
  private val lo4j2LOG = LogManager.getLogger(classOf[Nothing])

  override def flatMap(node: JsonNode, out: Collector[JsonNode]) = {
    val tuple = getKuduAndTsIndex
    val myKudu = tuple._1
    val _dwsynctsKuduFieldIndex = tuple._2

    breakable {
      val typeName = MyKuduTypeValue.getString(node, Type_field_name)
      if(typeName.equals(Type_value_events_track_signup))
        break

      val distinct_id = node.get(Distinct_id_field_name).asText()
      val dollaris_login_id = MyKuduTypeValue.getBool(node, Event_properties_dollaris_login_id)
      val user_id = MyKuduTypeValue.getLong(node, New_common_user_id_field_name)
      if(dollaris_login_id == null || user_id == null)
        break
      val updateMap = new util.HashMap[String, Object]()
      updateMap.put(New_users_id_field_name, user_id)

      val mystr_interkafka_ptoffset = node.get(MyName.MyName_mystr_interkafka_ptoffset).asText
      updateMap.put(MyName.MyName_mystr_interkafka_ptoffset, mystr_interkafka_ptoffset)

      if(dollaris_login_id) {
        updateMap.put(New_users_second_id_field_name, distinct_id)
      } else {
        updateMap.put(New_users_first_id_field_name, distinct_id)
      }
      myKudu.putStrAsKey(updateMap)

    }
    out.collect(node)
  }
}
