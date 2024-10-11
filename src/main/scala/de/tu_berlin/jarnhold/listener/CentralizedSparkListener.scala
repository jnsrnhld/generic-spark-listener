package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.EventType.EventType
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger

class CentralizedSparkListener(sparkConf: SparkConf) extends SparkListener {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CentralizedSparkListener])
  logger.info("Initializing CentralizedSparkListener")

  // Listener configuration
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.customExtraListener.isAdaptive", defaultValue = true)
  private val bridgeServiceAddress: String = sparkConf.get("spark.customExtraListener.bridgeServiceAddress")
  private val active: Boolean = this.isAdaptive

  // Setup communication
  checkConfigurations()
  private val zeroMQClient = new ZeroMQClient(bridgeServiceAddress)

  // Application parameters
  private val appSignature: String = sparkConf.get("spark.app.name")
  private var appId: String = _
  private val currentJobId = new AtomicInteger(0)
  private val currentScaleOut = new AtomicInteger(0)
  private var sparkContext: SparkContext = _

  def this(sparkConf: SparkConf, sparkContext: SparkContext) = {
    this(sparkConf)
    this.sparkContext = sparkContext
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (!this.active) {
      return
    }

    val jobId = jobStart.jobId
    this.currentJobId.set(jobId)
    if (isInitialJobOfSparkApplication(jobId)) {
      ensureSparkContextIsSet()
      setInitialScaleOut()
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
      val requestResult = this.sparkContext.requestTotalExecutors(recommendedScaleOut, 0, Map[String, Int]())
      logger.info("Request acknowledged? => " + requestResult.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (!this.active) {
      return
    }
    sendMessage(applicationEnd.time, EventType.APPLICATION_END)
    this.zeroMQClient.close()
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    handleScaleOutMonitoring(executorAdded.executorInfo.executorHost)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    handleScaleOutMonitoring("NO_HOST")
  }

  private def handleScaleOutMonitoring(executorHost: String): Unit = {
    synchronized {
      // the executors might be added before actual application start => there won't be a SparkContext yet in this case,
      // thus also no current scale out to monitor yet
      if (!this.active || this.sparkContext == null) {
        return
      }
      // An executor was removed? Else, an executor was added
      if (executorHost == "NO_HOST") {
        this.currentScaleOut.decrementAndGet()
      } else {
        this.currentScaleOut.incrementAndGet()
      }
    }
  }

  private def setInitialScaleOut(): Unit = {
    synchronized {
      val allExecutors = this.sparkContext.getExecutorMemoryStatus.toSeq.map(_._1)
      val driverHost: String = getDriverHost
      val scaleOut = allExecutors
        .filter(!_.split(":")(0).equals(driverHost))
        .toList
        .length
      this.currentScaleOut.set(scaleOut)
    }
  }

  private def checkConfigurations(): Unit = {
    val parametersList = List(
      "spark.customExtraListener.isAdaptive",
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
    this.zeroMQClient.sendMessage(message)
  }

  /**
   * Only call after ApplicationStart: Otherwise a second SparkContext will be created which will eventually crash
   * the application execution.
   */
  private def ensureSparkContextIsSet(): Unit = {
    if (this.sparkContext == null) {
      this.sparkContext = SparkContext.getOrCreate(this.sparkConf)
      logger.info("SparkContext successfully registered in CentralizedSparkListener")
    }
  }

  /**
   * Ensure spark context is set before calling.
   */
  private def getDriverHost = {
    this.sparkContext.getConf.get("spark.driver.host")
  }

  private def isInitialJobOfSparkApplication(jobId: Int) = {
    jobId == 0
  }
}
