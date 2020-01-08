package at.bronzels.track2dw

import at.bronzels.track2dw.sensordata.MySensorData.{Distinct_id_field_name, Event_field_name, Time_field_name}
import at.bronzels.track2dw.sensordata.MySensorData.{New_users_first_id_field_name, New_users_second_id_field_name}

object MyName {
  val MyName_mystr_uuid = "mystr_uuid"
    //VARCHAR WITH (primary_key=true),
    //VARCHAR
  val MyName_mystr_interkafka_ptoffset = "mystr_interkafka_ptoffset"
    //VARCHAR

  val MyName_mystr_set_by_track_signup = "mystr_set_by_track_signup"
    //BIGINT, only users

  //val MyName_mystr_dollarkafka_offset = "mystr_$kafka_offset"// BIGINT WITH (primary_key=true),

  val Events_primary_key_arr = Array(
    Distinct_id_field_name,
    Event_field_name,
    Time_field_name,
    MyName_mystr_uuid
  )

  val Users_primary_key_arr = Array(
    New_users_first_id_field_name,
    New_users_second_id_field_name
  )

}
