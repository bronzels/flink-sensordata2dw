package at.bronzels.sensordata2dw.flink

import java.util

import at.bronzels.libcdcdw.conf.{DistLockConf, KuduTableEnvConf}
import at.bronzels.libcdcdwstr.flink.util.{MyJackson, MyKuduTypeValue}
import at.bronzels.libcdcdwstr.flink.sink.Sink2KuduCommonMap
import at.bronzels.sensordata2dw.MyName
import at.bronzels.sensordata2dw.sensordata.MySensorData
import at.bronzels.sensordata2dw.sensordata.MySensorData._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

import scala.util.control.Breaks.{break, breakable}

class SensorDataUsersIdAddedJsonFieldsCleanedNode2KuduMap(override val kuduTableEnvConf: KuduTableEnvConf, override val distLockConf: DistLockConf) extends Sink2KuduCommonMap[JsonNode, JsonNode](kuduTableEnvConf, distLockConf, MySensorData.isSrcFieldNameWTUpperCase) {
  override def flatMap(node: JsonNode, out: Collector[JsonNode]) = {
    val tuple = getKuduAndTsIndex
    val myKudu = tuple._1
    val _dwsynctsKuduFieldIndex = tuple._2

    breakable {
      val distinc_id = node.get(Distinct_id_field_name).asText()
      val typeName = node.get(Type_field_name).asText()
      if(typeName.equals(Type_value_events_track_signup)) {
        val distinct_id = node.get(Distinct_id_field_name).asText()
        val original_id = MyKuduTypeValue.getString(node, Event_track_signup_original_id_field_name)
        val user_id = MyKuduTypeValue.getLong(node, New_common_user_id_field_name)
        if (original_id == null || user_id == null)
          break

        val updateMap = new util.HashMap[String, Object]()
        updateMap.put(New_users_id_field_name, user_id)
        updateMap.put(New_users_second_id_field_name, distinct_id)

        val conditionFirstMap = new util.HashMap[String, Object]()
        conditionFirstMap.put(New_users_first_id_field_name, original_id)
        val selectedFirstList = myKudu.getSelectedAll(conditionFirstMap)
        val size = selectedFirstList.size()
        if(size > 0) {
          if(size > 1) {

          }
          val selectedMap = selectedFirstList.get(0);

          val conditionIdMap = new util.HashMap[String, Object]()
          conditionIdMap.put(New_users_id_field_name, selectedMap.get(New_users_id_field_name))
          myKudu.deleteStrAsKey(conditionIdMap)

          selectedMap.remove(New_users_id_field_name)
          //selectedMap.remove(New_users_first_id_field_name)
          selectedMap.remove(New_users_second_id_field_name)

          selectedMap.remove(at.bronzels.libcdcdw.Constants.FIELDNAME_MODIFIED_TS)
          selectedMap.remove(MyName.MyName_mystr_interkafka_ptoffset)

          updateMap.putAll(selectedMap)
        } else {
          updateMap.put(New_users_first_id_field_name, original_id)
        }

        val mystr_interkafka_ptoffset = node.get(MyName.MyName_mystr_interkafka_ptoffset).asText
        updateMap.put(MyName.MyName_mystr_interkafka_ptoffset, mystr_interkafka_ptoffset)

        myKudu.putStrAsKey(updateMap)
      } else {
        /*
        val additionalFieldSet = new util.HashSet[String]()
        additionalFieldSet.add(New_users_id_field_name)
        val conditionSecondMap = new util.HashMap[String, Object]()
        conditionSecondMap.put(New_users_second_id_field_name, distinc_id)
        val selectedSecond = myKudu.getSelected(conditionSecondMap, additionalFieldSet)
        if(selectedSecond.size() == 0) {
          val conditionFirstMap = new util.HashMap[String, Object]()
          conditionFirstMap.put(New_users_first_id_field_name, distinc_id)
          val selectedFirst = myKudu.getSelected(conditionFirstMap, additionalFieldSet)
          if(selectedFirst.size() == 0) {
            break
          } else {
            val id = selectedFirst.get(0).get(New_users_id_field_name).asInstanceOf[String]
            node.asInstanceOf[ObjectNode].put(New_users_id_field_name, id)
          }
        } else {
          val id = selectedSecond.get(0).get(New_users_id_field_name).asInstanceOf[String]
          node.asInstanceOf[ObjectNode].put(New_users_id_field_name, id)
        }
        */
        out.collect(node)
      }
    }
  }
}
