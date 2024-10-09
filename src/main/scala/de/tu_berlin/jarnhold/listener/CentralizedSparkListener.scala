package de.tu_berlin.jarnhold.listener

import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import EventType.EventType
import org.json4s.{DefaultFormats, Formats};

class CentralizedSparkListener(sparkConf: SparkConf, sparkContext: SparkContext) extends SparkListener {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CentralizedSparkListener])
  logger.info("Initializing CentralizedSparkListener")

  // Listener configuration
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.customExtraListener.isAdaptive", defaultValue = true)
  private val bridgeServiceAddress: String = sparkConf.get("spark.customExtraListener.bridgeServiceAddress")
  private val active: Boolean = this.isAdaptive

  // Setup communication
  checkConfigurations()
  implicit val formats: Formats = DefaultFormats
  private val zeroMQClient = new ZeroMQClient(bridgeServiceAddress)(formats)

  // Application parameters
  private val appSignature: String = sparkConf.get("spark.app.name")
  private var appId: String = _
  private val currentJobId = new AtomicInteger(0)
  private val currentScaleOut = new AtomicInteger(0)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (!this.active) {
      return
    }

    val jobId = jobStart.jobId
    this.currentJobId.set(jobId)
    if (jobId == 0) {
      handleScaleOutMonitoring(None)
    }

    val response = sendMessage(jobStart.time, EventType.JOB_START)
    this.appId = response.app_id
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (!this.active) {
      return
    }
    val jobDuration = jobEnd.time
    val response = sendMessage(jobDuration, EventType.JOB_END)

    val recommendedScaleOut = response.recommended_scale_out
    if (recommendedScaleOut != this.currentScaleOut.get()) {
      logger.info(s"Requesting scale-out of $recommendedScaleOut after next job...")
      val requestResult = sparkContext.requestTotalExecutors(recommendedScaleOut, 0, Map[String, Int]())
      logger.info("Request acknowledged? => " + requestResult.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (!this.active) {
      return
    }
    sendMessage(applicationEnd.time, EventType.APPLICATION_END)
    zeroMQClient.close()
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    handleScaleOutMonitoring(Option(executorAdded.executorInfo.executorHost))
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    handleScaleOutMonitoring(Option("NO_HOST"))
  }

  private def handleScaleOutMonitoring(executorHost: Option[String]): Unit = {
    synchronized {
      if (!this.active) {
        return
      }
      // No scale-out yet? Get the number of currently running executors
      if (this.currentScaleOut.get() == 0) {
        this.currentScaleOut.set(getInitialScaleOutCount(executorHost.getOrElse("NO_HOST")))
      }
      // An executor was removed? Else, an executor was added
      if (executorHost.isDefined && executorHost.get == "NO_HOST") {
        this.currentScaleOut.decrementAndGet()
      } else if (executorHost.isDefined) {
        this.currentScaleOut.incrementAndGet()
      }
    }
  }

  private def getInitialScaleOutCount(executorHost: String): Int = {
    val allExecutors = sparkContext.getExecutorMemoryStatus.toSeq.map(_._1)
    val driverHost: String = sparkContext.getConf.get("spark.driver.host")
    allExecutors
      .filter(!_.split(":")(0).equals(driverHost))
      .filter(!_.split(":")(0).equals(executorHost.split(":")(0)))
      .toList
      .length
  }

  private def checkConfigurations(): Unit = {
    val parametersList = List(
      "spark.customExtraListener.bridgeServiceAddress"
    )
    logger.info("Current spark conf" + sparkConf.toDebugString)
    for (param <- parametersList) {
      if (!sparkConf.contains(param)) {
        throw new IllegalArgumentException(s"Parameter $param is not specified in the environment!")
      }
    }
  }

  private def sendMessage(appTime: Long, eventType: EventType): ResponseMessage = {
    val message = RequestMessage(
      app_id = this.appId,
      app_name = this.appSignature,
      app_time = appTime,
      job_id = this.currentJobId.get(),
      num_executors = this.currentScaleOut.get(),
      event_type = eventType
    )
    zeroMQClient.sendMessage(message)
  }
}
