package at.bronzels.track2dw.flink

import java.util

import at.bronzels.libcdcdw.conf.{DistLockConf, KuduTableEnvConf}
import at.bronzels.libcdcdw.util.MyString
import at.bronzels.libcdcdwstr.flink.util.MyJackson
import at.bronzels.libcdcdwstr.flink.FrameworkScalaInf
import at.bronzels.libcdcdwstr.flink.bean.ScalaStreamContext
import at.bronzels.libcdcdwstr.flink.source.KafkaJsonNodeScalaStream
import at.bronzels.track2dw.CliInput
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

object TrackDataToDatawarehouse {

  val logger: Logger  = LoggerFactory.getLogger("TrackDataToDatawarehouse")
  val sensorDataEventsTableName = "sensordata_events"
  val sensorDataUsersTableName = "sensordata_users"

  val appName = "track2dw"

  def main(args: Array[String]): Unit = {
    //val newargs = args
    val newargs = Array[String](
      //"-f", "4",
      //dw_v_0_0_1_201912_2320::bd_jsd.sensordata_events_fm
      //"-sdtp", "event_topic",
      "-sdtp", "event_topic.mirror",

      "-sdpc", "2",
      "-sdpn", "fm",
      "-sdzkq", "beta-hbase02:2181,beta-hbase03:2181,beta-hbase04:2181/kafka3",
      "-sdbstr", "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092",
      //"-sdzkq", "data01.weiju.sa:2181",
      //"-sdbstr", "data01.weiju.sa:9092",

      "-sdkdcat", "bd",
      "-sdkdurl", "beta-hbase01:7051",
      "-sdkddb", "dw_v_0_0_1_20191223_1830",

      "-sddlurl", "beta-hbase01:6379",
      "-suut",
      "-susidf"
      //"-sddlurl", "beta-hbase02:2181,beta-hbase03:2181,beta-hbase04:2181"
    )


    val myCli = new CliInput
    val options = myCli.buildOptions()

    if (myCli.parseIsHelp(options, newargs)) return

    val prefixedStrNameCliInput = appName + at.bronzels.libcdcdw.Constants.commonSep + myCli.getStrNameCliInput
    FrameworkScalaInf.launchedMS = System.currentTimeMillis()
    FrameworkScalaInf.appName = prefixedStrNameCliInput

    var flinkInputParallelism4Local: java.lang.Integer = null
    if (myCli.isFlinkInputLocalMode)
      flinkInputParallelism4Local = myCli.getFlinkInputParallelism4Local
    FrameworkScalaInf.setupEnv(flinkInputParallelism4Local, myCli.isOperatorChainDisabled)

    val streamContext = new ScalaStreamContext(FrameworkScalaInf.env, FrameworkScalaInf.tableEnv)

    val cdcPrefix = myCli.getCdcPrefix
    val outputPrefix = myCli.getOutputPrefix

    val strZkQuorum = myCli.kafkaZKQuorum
    val strBrokers = myCli.kafkaBootstrap
    val kuduCatalog = myCli.kuduCatalog
    val kuduUrl = myCli.kuduUrl
    val kuduDatabase = myCli.kuduDatabase

    val eventsTableName: String = MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep,
      sensorDataEventsTableName,
      myCli.getCdcPrefix,
      myCli.projectName)
    val usersTableName: String = MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep,
      sensorDataUsersTableName,
      myCli.getCdcPrefix,
      myCli.projectName)

    val eventsKuduTableEnvConf = new KuduTableEnvConf(kuduDatabase, kuduUrl, kuduCatalog, eventsTableName)
    val usersKuduTableEnvConf = new KuduTableEnvConf(kuduDatabase, kuduUrl, kuduCatalog, usersTableName)

    val kafkaJsonNodeStreamObj = new KafkaJsonNodeScalaStream(outputPrefix, prefixedStrNameCliInput, streamContext, strZkQuorum, strBrokers, util.Collections.singletonList(myCli.topicName))

    val kafkaJsonNodeStream = kafkaJsonNodeStreamObj.getStream
    //kafkaJsonNodeStream.print()

    val streamTuple = TrackDataStream.getStreamJsonedFilteredFlattendedSplitted(kafkaJsonNodeStream, myCli.projectId, myCli.isUpdateUserTable)
    val streamEvents = streamTuple._1
    streamEvents.map(node => MyJackson.getString(node)).print()

    val distLockConf = new DistLockConf(myCli.distLockUrl, MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep, appName, myCli.getCdcPrefix, myCli.projectName))

    val eventsStreamTuple = TrackDataStream.getEventsLogedinPutSinkedTupleStream(streamEvents, eventsKuduTableEnvConf, usersKuduTableEnvConf, distLockConf)
    val eventsLogedinBulkUpdateStream = eventsStreamTuple._1
    //eventsLogedinBulkUpdateStream.print()
    val eventsSinkedStream = eventsStreamTuple._2
    eventsSinkedStream.print()

    if (myCli.isUpdateUserTable) {
      val streamUsers = streamTuple._2
      //streamUsers.map(node => MyJackson.getString(node)).print()
      val usersSinkedStream = TrackDataStream.getUsersSinkedStream(streamUsers, usersKuduTableEnvConf, distLockConf, myCli.isOnlySaveUserIdsField)
      //usersSinkedStream.print()
      FrameworkScalaInf.env.execute(prefixedStrNameCliInput)
    }
  }

}
