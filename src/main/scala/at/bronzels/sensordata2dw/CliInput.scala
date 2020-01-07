package at.bronzels.sensordata2dw

import at.bronzels.libcdcdwstr.SimpleCommonCliInput
import org.apache.commons.cli.{Option, Options}

class CliInput extends SimpleCommonCliInput {
  val sensorDataTopicName = "event_topic"

  var topicName:String = sensorDataTopicName

  var projectId:Long = -1L
  var projectName:String = _

  var kafkaZKQuorum:String = _
  var kafkaBootstrap:String = _

  var kuduCatalog:String = _
  var kuduUrl:String = _
  var kuduDatabase:String = _

  var distLockUrl:String = _

  override def buildOptions(): Options = {
    val options = super.buildOptions()

    //topic name
    val optionTopicName = new Option("sdtp", "topicName", true, "sensor data topic name")
    optionTopicName.setRequired(true);options.addOption(optionTopicName)

    //projectId
    val optionProjectId = new Option("sdpc", "projectCode", true, "project code specified in sensor web portal and used in streaming to filter log msg only for this project")
    optionProjectId.setRequired(true);options.addOption(optionProjectId)

    //project name
    val optionProjectName = new Option("sdpn", "projectName", true, "project name specified in sensor web portal and used in streaming to decide dest datawarehouse table name")
    options.addOption(optionProjectName)

    //kafka zookeeper quorum(with kafka path)
    val optionKafkaZKQuorum = new Option("sdzkq", "kafkaZKQuorum", true, "kafka zookeeper quorum with zk host:port concatted with , and end with kafka path")
    optionKafkaZKQuorum.setRequired(true);options.addOption(optionKafkaZKQuorum)

    //kafka bootstrap hosts/ports
    val optionKafkaBootstrapServers = new Option("sdbstr", "kafkaBootstrap", true, "kafka bootstrap servers with kafka daemon host:port concatted with ,")
    optionKafkaBootstrapServers.setRequired(true);options.addOption(optionKafkaBootstrapServers)

    //kudu catalog name
    val optionKuduCatalog = new Option("sdkdcat", "kuduCatalog", true, "kudu catalog name used before :: in table name, usually presto")
    optionKuduCatalog.setRequired(true);options.addOption(optionKuduCatalog)

    //kudu url
    val optionKuduUrl = new Option("sdkdurl", "kuduUrl", true, "kudu host:port")
    optionKuduUrl.setRequired(true);options.addOption(optionKuduUrl)

    //kudu database
    val optionKuduDatabase = new Option("sdkddb", "kuduDatabase", true, "kudu database name to simulate multiple db")
    optionKuduDatabase.setRequired(true);options.addOption(optionKuduDatabase)

    //redis url
    val optionRedisUrl = new Option("sddlurl", "distLockUrl", true, "redis/zookeeper host:port/host1:port1,host2:port2")
    optionRedisUrl.setRequired(true);options.addOption(optionRedisUrl)

    options
  }

  override def parseIsHelp(options: Options, args: Array[String]): Boolean = {
    if(super.parseIsHelp (options, args))
      return true

    val comm = getCommandLine(options, args)

    if (comm.hasOption("sdtp")) {
      topicName = comm.getOptionValue("sdtp")
    }

    if (comm.hasOption("sdpc")) {
      val strProjectId = comm.getOptionValue("sdpc")
      projectId = strProjectId.toLong
    }

    if(projectId.equals(-1L))
      throw new RuntimeException("valid projectId input is mandatory")

    if (comm.hasOption("sdpn")) {
      projectName = comm.getOptionValue("sdpn")
    }

    if (comm.hasOption("sdzkq")) {
      kafkaZKQuorum = comm.getOptionValue("sdzkq")
    }
    if (comm.hasOption("sdbstr")) {
      kafkaBootstrap = comm.getOptionValue("sdbstr")
    }

    if (comm.hasOption("sdkdcat")) {
      kuduCatalog = comm.getOptionValue("sdkdcat")
    }
    if (comm.hasOption("sdkdurl")) {
      kuduUrl = comm.getOptionValue("sdkdurl")
    }
    if (comm.hasOption("sdkddb")) {
      kuduDatabase = comm.getOptionValue("sdkddb")
    }

    if (comm.hasOption("sddlurl")) {
      distLockUrl = comm.getOptionValue("sddlurl")
    }

    return false

  }

}
