package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.SafeDivision.saveDivision
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Create a spark listener emitting spark events via a ZeroMQ client.
 *
 * @param sparkConf Is injected automatically to listener when added via "spark.extraListeners".
 */
class CentralizedSparkListener(sparkConf: SparkConf) extends SparkListener {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CentralizedSparkListener])
  logger.info("Initializing CentralizedSparkListener")

  // Listener configuration
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.customExtraListener.isAdaptive", defaultValue = true)
  private val bridgeServiceAddress: String = sparkConf.get("spark.customExtraListener.bridgeServiceAddress")
  private val active: Boolean = this.isAdaptive
  private val executorRequestTimeout: Integer = Option(System.getenv("EXECUTOR_REQUEST_TIMEOUT")).getOrElse("15000").toInt
  private val executorRequestStopWatch: StopWatch = StopWatch.create()

  // Setup communication
  private val zeroMQClient = new ZeroMQClient(bridgeServiceAddress)

  // Application parameters
  private val applicationId: String = sparkConf.getAppId
  private val appSignature: String = sparkConf.get("spark.app.name")
  private var appEventId: String = _
  private var appStartTime: Long = _
  private var sparkContext: SparkContext = _
  private var initialScaleOut: Integer = _
  private val lastKnownScaleOut = new AtomicInteger(0)

  // scale-out, time of measurement, total time
  private val scaleOutBuffer: ListBuffer[(Int, Long)] = ListBuffer()
  // stage monitoring
  private val stageInfoMap: StageInfoMap = new StageInfoMap()

  /**
   * For testing purposes.
   */
  def this(sparkConf: SparkConf, sparkContext: SparkContext) = {
    this(sparkConf)
    this.sparkContext = sparkContext
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

    checkConfigurations(this.sparkConf)
    this.sparkContext = SparkContext.getOrCreate(this.sparkConf)

    val response = sendAppStartMessage()
    this.appEventId = response.app_event_id
    this.appStartTime = appStartTime
    this.initialScaleOut = response.recommended_scale_out

    logger.info(
      "SparkContext successfully registered in CentralizedSparkListener and executor recommendation received. "
        + "Requesting {} executors", this.initialScaleOut
    )
    this.executorRequestStopWatch.start()
    this.sparkContext.requestTotalExecutors(this.initialScaleOut, 0, Map[String, Int]())
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val response: ResponseMessage =
      if (isInitialJobOfSparkApplication(jobStart.jobId)) {
        sendJobStartMessage(jobStart.jobId, jobStart.time, this.initialScaleOut)
      } else {
        sendJobStartMessage(jobStart.jobId, jobStart.time, getScaleOutFromSparkContext)
      }
    this.stageInfoMap.addJob(jobStart)
    adjustScaleOutIfNecessary(response)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val scaleOut = this.lastKnownScaleOut.get()
    this.stageInfoMap.addStageSubmit(scaleOut, stageSubmitted)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val rescalingTimeRatio = computeRescalingTimeRatio(
      stageCompleted.stageInfo.submissionTime.getOrElse(0L),
      stageCompleted.stageInfo.completionTime.getOrElse(0L)
    )
    val scaleOut = this.lastKnownScaleOut.get()
    this.stageInfoMap.addStageComplete(scaleOut, rescalingTimeRatio, stageCompleted)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobDuration = jobEnd.time
    val rescalingTimeRatio: Double = computeRescalingTimeRatio(this.appStartTime, jobEnd.time)
    val stages = this.stageInfoMap.getStages(jobId)
    val response = sendJobEndMessage(jobId, jobDuration, rescalingTimeRatio, stages)
    adjustScaleOutIfNecessary(response)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    this.sendAppEndMessage(applicationEnd.time)
    this.zeroMQClient.close()
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    handleScaleOutMonitoring(executorAdded.time)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    handleScaleOutMonitoring(executorRemoved.time)
  }

  private def adjustScaleOutIfNecessary(response: ResponseMessage): Unit = {
    val recommendedScaleOut = response.recommended_scale_out

    logger.info(
      "Will adjust scale-out if necessary: {}, recommended scale-out: {} last-known scale-out: {}",
      (recommendedScaleOut != this.lastKnownScaleOut.get()).toString,
      recommendedScaleOut.toString,
      this.lastKnownScaleOut.get().toString
    )

    if (recommendedScaleOut != this.lastKnownScaleOut.get()) {

      if (!this.executorRequestStopWatch.isStopped) {
        this.executorRequestStopWatch.stop()
      }
      // for fast jobs, we don't want to request different amounts of executors too often
      if (this.executorRequestStopWatch.getTime(TimeUnit.MILLISECONDS) < this.executorRequestTimeout) {
        logger.info(
          "Too frequent requests...Stop-watch time: {} will not request additional executors",
          this.executorRequestStopWatch.getTime(TimeUnit.MILLISECONDS)
        )
        this.executorRequestStopWatch.reset()
        this.executorRequestStopWatch.start()
        return
      }

      logger.info(s"Requesting scale-out of $recommendedScaleOut after next job...")
      val requestResult = this.sparkContext.requestTotalExecutors(recommendedScaleOut, 0, Map[String, Int]())
      logger.info("Request acknowledged? => " + requestResult.toString)
      this.executorRequestStopWatch.reset()
      this.executorRequestStopWatch.start()
    }
  }

  private def handleScaleOutMonitoring(executorActionTime: Long): Unit = {
    synchronized {
      val scaleOut = getScaleOutFromSparkContext
      scaleOutBuffer.append((scaleOut, executorActionTime))
    }
  }

  /**
   * Ensure spark context is set before calling.
   */
  private def getScaleOutFromSparkContext: Int = {

    // last job event might be emitted after context is already terminated
    if (this.sparkContext.isStopped) {
      return this.lastKnownScaleOut.get()
    }

    val allExecutors = this.sparkContext.getExecutorMemoryStatus.toSeq.map(_._1)
    val driverHost: String = getDriverHost
    val currentScaleOut = allExecutors
      .filter(!_.split(":")(0).equals(driverHost))
      .toList
      .length

    this.lastKnownScaleOut.set(currentScaleOut)
    currentScaleOut
  }

  private def checkConfigurations(sparkConf: SparkConf): Unit = {
    logger.info("Current spark conf" + sparkConf.toDebugString)
    for (param <- SpecBuilder.requiredSparkConfParams) {
      if (!sparkConf.contains(param)) {
        throw new IllegalArgumentException(s"Parameter $param is not specified in the environment!")
      }
    }
  }

  private def computeRescalingTimeRatio(startTime: Long, endTime: Long): Double = {

    val scaleOutList: List[(Int, Long)] = scaleOutBuffer.toList

    val dividend: Long = scaleOutList
      .sortBy(_._2)
      .zipWithIndex.map { case (tup, idx) => (tup._1, tup._2, Try(scaleOutList(idx + 1)._2 - tup._2).getOrElse(0L)) }
      .filter(e => e._2 + e._3 >= startTime && e._2 <= endTime)
      .drop(1) // drop first element => is respective start scale-out
      .dropRight(1) // drop last element => is respective end scale-out
      .map(e => {
        val startTimeScaleOut: Long = e._2
        var endTimeScaleOut: Long = e._2 + e._3
        if (e._3 == 0L)
          endTimeScaleOut = endTime

        val intervalStartTime: Long = Math.min(Math.max(startTime, startTimeScaleOut), endTime)
        val intervalEndTime: Long = Math.max(startTime, Math.min(endTime, endTimeScaleOut))

        intervalEndTime - intervalStartTime
      })
      .sum

    saveDivision(dividend, endTime - startTime)
  }

  private def sendJobStartMessage(jobId: Int, appTime: Long, scaleOut: Int): ResponseMessage = {
    val message = JobStartMessage(
      app_event_id = this.appEventId,
      app_time = appTime,
      job_id = jobId,
      num_executors = scaleOut,
    )
    this.zeroMQClient.sendMessage(EventType.JOB_START, message)
  }

  private def sendJobEndMessage(jobId: Int, appTime: Long, rescalingTimeRatio: Double, stages: Map[String, Stage]): ResponseMessage = {
    val message = JobEndMessage(
      app_event_id = this.appEventId,
      app_time = appTime,
      job_id = jobId,
      num_executors = getScaleOutFromSparkContext,
      rescaling_time_ratio = rescalingTimeRatio,
      stages = stages
    )
    this.zeroMQClient.sendMessage(EventType.JOB_END, message)
  }

  private def sendAppStartMessage(): ResponseMessage = {
    val specBuilder = new SpecBuilder(this.sparkConf)
    val message = AppStartMessage(
      application_id = this.applicationId,
      app_name = this.appSignature,
      app_time = System.currentTimeMillis(),
      is_adaptive = this.isAdaptive,
      app_specs = specBuilder.buildAppSpecs(),
      driver_specs = specBuilder.buildDriverSpecs(),
      executor_specs = specBuilder.buildExecutorSpecs(),
      environment_specs = specBuilder.buildEnvironmentSpecs(),
    )
    this.zeroMQClient.sendMessage(EventType.APPLICATION_START, message)
  }

  private def sendAppEndMessage(appTime: Long): ResponseMessage = {
    val message = AppEndMessage(
      app_event_id = this.appEventId,
      app_time = appTime,
      num_executors = getScaleOutFromSparkContext
    )
    this.zeroMQClient.sendMessage(EventType.APPLICATION_END, message)
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
