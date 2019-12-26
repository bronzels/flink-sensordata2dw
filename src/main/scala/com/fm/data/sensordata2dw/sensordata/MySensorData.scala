package com.fm.data.sensordata2dw.sensordata

import com.fm.data.sensordata2dw.dw.DWName

object MySensorData {
  val isSrcFieldNameWTUpperCase = true

  //common
  val Distinct_id_field_name = "distinct_id"
  val Type_field_name = "type"
  val Time_field_name = "time"
  val Project_id_field_name = "project_id"
  val Project_name_field_name = "project"

  //events specific
  val Event_field_name = "event"
  //track_signup type specific
  val Event_track_signup_original_id_field_name = "original_id"
  val Event_properties_dollaris_login_id = "$is_login_id"

  val Properties_field_name = "properties"

  val Type_value_users_profile_set = "profile_set"
  val Type_value_users_profile_set_once = "profile_set_once"
  val Type_value_users_profile_increment = "profile_increment"
  val Type_value_users_profile_delete = "profile_delete"
  val Type_value_users_profile_append = "profile_append"
  val Type_value_users_profile_unset = "profile_unset"

  val Type_value_users_arr = Array(
    Type_value_users_profile_set,
    Type_value_users_profile_set_once,
    Type_value_users_profile_increment,
    Type_value_users_profile_delete,
    Type_value_users_profile_append,
    Type_value_users_profile_unset
  )

  def isUserSpecific(eventTypeName: String): Boolean = {
    Type_value_users_arr.contains(eventTypeName)
  }

  val Type_value_items_item_set = "item_set"
  val Type_value_items_item_delete = "item_delete"

  val Type_value_items_arr = Array(
    Type_value_items_item_set,
    Type_value_items_item_delete
  )

  def isItemSpecific(eventTypeName: String): Boolean = {
    Type_value_events_arr.contains(eventTypeName)
  }

  val Type_value_events_track = "track"
  val Type_value_events_track_signup = "track_signup"

  val Type_value_events_arr = Array(
    Type_value_events_track,
    Type_value_events_track_signup
  )

  def isEventSpecific(eventTypeName: String): Boolean = {
    Type_value_events_arr.contains(eventTypeName)
  }

  val New_common_user_id_field_name = "user_id"

  val New_users_id_field_name = "id"
  val New_users_first_id_field_name = "first_id"
  val New_users_second_id_field_name = "second_id"

  val Reserved_table_name_events = "events"
  val Reserved_table_name_users = "users"

  val Reserved_field_name_arr = Array(
    DWName.DW_reserved_name_date,
    DWName.DW_reserved_name_datetime,

    Distinct_id_field_name,

    Event_field_name,

    Reserved_table_name_events,

    New_users_first_id_field_name,
    New_users_id_field_name,

    Event_track_signup_original_id_field_name,

    "device_id",

    Properties_field_name,

    New_users_second_id_field_name,

    Time_field_name,

    New_common_user_id_field_name,
    Reserved_table_name_users
  )

  /*
  def getIDFromDeviceLogined(deviceId: String, loginedId: String): String = {
    val concatted = MyString.concatBySkippingEmpty(com.fm.data.libcommon.Constants.commaSep, deviceId, loginedId)
    concatted
    val md5ed = DigestUtils.md5Hex(concatted)
    val reConcatted = MyString.concatBySkippingEmpty(com.fm.data.libcommon.Constants.commaSep, concatted, md5ed)
    reConcatted
  }
   */

  val Useless_top_layer_exclude_field_Arr = Array(
    "_track_id",
    "_flush_time",
    "map_id",
    "lib",
    "extractor",
    "ver",

    "",
    "",
    ""
  )

  val Useless_properties_layer_exclude_field_Arr = Array(
    "",
    ""
  )

  val Events_at_last_exclude_field_Arr = Array(
    Project_id_field_name,
    Project_name_field_name
  )

  val Users_at_last_exclude_field_Arr = Array(
    Type_field_name,
    Time_field_name,
    Project_id_field_name,
    Project_name_field_name,
    Distinct_id_field_name
  )

}
