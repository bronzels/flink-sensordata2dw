package at.bronzels.sensordata2dw.flink

import java.util

import at.bronzels.libcdcdw.conf.{KuduTableEnvConf, DistLockConf}
import at.bronzels.libcdcdw.util.MyString
import at.bronzels.libcdcdwstr.flink.util.MyJackson
import at.bronzels.libcdcdwstr.flink.FrameworkScalaInf
import at.bronzels.libcdcdwstr.flink.bean.ScalaStreamContext
import at.bronzels.libcdcdwstr.flink.source.KafkaJsonNodeScalaStream
import at.bronzels.sensordata2dw.CliInput
import org.apache.flink.streaming.api.scala._

object SensorDataToDatawarehouse {
  val sensorDataEventsTableName = "sensordata_events"
  val sensorDataUsersTableName = "sensordata_users"

  val appName = "sensordata2dw"

  def main(args: Array[String]): Unit = {
    //val newargs = args
    val newargs = Array[String](
      "-f", "4",

      "-sdtp", "event_topic",

      "-sdpc", "2",
      "-sdpn", "fm",

      /*
      "-sdzkq", "beta-hbase02:2181,beta-hbase03:2181,beta-hbase04:2181/kafka",
      "-sdbstr", "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092",
       */
      "-sdzkq", "data01.weiju.sa:2181",
      "-sdbstr", "data01.weiju.sa:9092",

      "-sdkdcat", "presto",
      "-sdkdurl", "beta-hbase01:7051",
      "-sdkddb", "dw_v_0_0_1_20191111_1535",

      "-sddlurl", "beta-hbase01:6379"
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

    var eventsTableName: String = MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep,
      sensorDataEventsTableName,
      myCli.getCdcPrefix,
      myCli.projectName)
    var usersTableName: String = MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep,
      sensorDataUsersTableName,
      myCli.getCdcPrefix,
      myCli.projectName)

    val eventsKuduTableEnvConf = new KuduTableEnvConf(kuduCatalog, kuduUrl, kuduDatabase, eventsTableName)
    val usersKuduTableEnvConf = new KuduTableEnvConf(kuduCatalog, kuduUrl, kuduDatabase, usersTableName)

    val kafkaJsonNodeStreamObj = new KafkaJsonNodeScalaStream(outputPrefix, prefixedStrNameCliInput, streamContext, strZkQuorum, strBrokers, util.Collections.singletonList(myCli.topicName))

    val kafkaJsonNodeStream = kafkaJsonNodeStreamObj.getStream
    //kafkaJsonNodeStream.print()

    val streamTuple = SensorDataStream.getStreamJsonedFilteredFlattendedSplitted(kafkaJsonNodeStream, myCli.projectId)
    val streamEvents = streamTuple._1
    //streamEvents.map(node => MyJackson.getString(node)).print()

    val distLockConf = new DistLockConf(myCli.distLockUrl, MyString.concatBySkippingEmpty(at.bronzels.libcdcdw.Constants.commonSep, appName, myCli.getCdcPrefix, myCli.projectName))

    val eventsStreamTuple = SensorDataStream.getEventsLogedinPutSinkedTupleStream(streamEvents, eventsKuduTableEnvConf, usersKuduTableEnvConf, distLockConf)
    val eventsLogedinBulkUpdateStream = eventsStreamTuple._1
    //eventsLogedinBulkUpdateStream.print()
    val eventsSinkedStream = eventsStreamTuple._2
    //eventsSinkedStream.print()

    val streamUsers = streamTuple._2
    //streamUsers.map(node => MyJackson.getString(node)).print()

    val usersSinkedStream = SensorDataStream.getUsersSinkedStream(streamUsers, usersKuduTableEnvConf, distLockConf)
    //usersSinkedStream.print()

    FrameworkScalaInf.env.execute(prefixedStrNameCliInput)

  }

}
