package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.SafeDivision.saveDivision
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.storage.RDDInfo

import java.util.concurrent.ConcurrentHashMap

class StageInfoMap {

  private val submits: ConcurrentHashMap[Int, Array[StageSubmit]] =
    new ConcurrentHashMap[Int, Array[StageSubmit]]()
  private val completes: ConcurrentHashMap[Int, Array[StageComplete]] =
    new ConcurrentHashMap[Int, Array[StageComplete]]()

  def addStages(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    this.submits.put(jobId, Array.ofDim[StageSubmit](jobStart.stageInfos.size))
    this.completes.put(jobId, Array.ofDim[StageComplete](jobStart.stageInfos.size))
  }

  def addStageSubmit(jobId: Int, executorCount: Int, stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    // stage-ids are incremental integers, but 1-index based
    this.submits.get(jobId){stageInfo.stageId - 1} = StageSubmit(
      stageInfo.stageId,
      stageInfo.name,
      stageInfo.submissionTime.get,
      executorCount,
      stageInfo.parentIds.toArray,
      stageInfo.numTasks,
    )
  }

  def addStageComplete(
                        jobId: Int,
                        executorCount: Int,
                        rescalingTimeRatio: Double,
                        stageCompleted: SparkListenerStageCompleted
                      ): Unit = {
    val stageInfo = stageCompleted.stageInfo
    // stage-ids are incremental integers, but 1-index based
    this.completes.get(jobId){stageInfo.stageId - 1} = StageComplete(
      stageInfo.stageId,
      stageInfo.name,
      stageInfo.completionTime.get,
      executorCount,
      rescalingTimeRatio,
      stageInfo.attemptNumber(),
      stageInfo.failureReason.getOrElse(""),
      extractFromRDD(stageInfo.rddInfos),
      extractFromTaskMetrics(stageInfo.taskMetrics)
    )
  }

  def getStages(jobId: Int): Array[Stage] = {
    if (this.submits.size() != this.completes.size()) {
      throw new IllegalStateException("Amount of submits and completes not equal. Cannot construct stages.")
    }
    val submits = this.submits.get(jobId)
    val completes = this.completes.get(jobId)
    submits.zipWithIndex.map { case (stageSubmit: StageSubmit, index) =>
      val stageComplete = completes{index}
      Stage(
        stage_id = f"${stageSubmit.id}",
        stage_name = stageSubmit.name,
        num_tasks = stageSubmit.numTasks,
        parent_stage_ids = stageSubmit.parentIds.map(id => f"${id}").mkString(","),
        attempt_id = stageComplete.attemptId,
        failure_reason = stageComplete.failureReason,
        start_time = stageSubmit.time,
        end_time = stageComplete.time,
        start_scale_out = stageSubmit.scaleOut,
        end_scale_out = stageComplete.scaleOut,
        rescaling_time_ratio = stageComplete.rescalingTimeRation,
        rdd_num_partitions = stageComplete.rddInfo.numPartitions,
        rdd_num_cached_partitions = stageComplete.rddInfo.numCachedPartitions,
        rdd_mem_size = stageComplete.rddInfo.memSize,
        rdd_disk_size = stageComplete.rddInfo.diskSize,
        metrics = stageComplete.stageMetrics
      )
    }
  }

  private def extractFromRDD(seq: Seq[RDDInfo]): StageRddInfo = {
    var numPartitions: Int = 0
    var numCachedPartitions: Int = 0
    var memSize: Long = 0L
    var diskSize: Long = 0L
    seq.foreach(rdd => {
      numPartitions += rdd.numPartitions
      numCachedPartitions += rdd.numCachedPartitions
      memSize += rdd.memSize
      diskSize += rdd.diskSize
    })
    StageRddInfo(numPartitions, numCachedPartitions, memSize, diskSize)
  }

  private def extractFromTaskMetrics(taskMetrics: TaskMetrics): StageMetrics = {
    // cpu time is nanoseconds, run time is milliseconds
    val cpuUtilization: Double = saveDivision(taskMetrics.executorCpuTime, (taskMetrics.executorRunTime * 1000000))
    val gcTimeRatio: Double = saveDivision(taskMetrics.jvmGCTime, taskMetrics.executorRunTime)
    val shuffleReadWriteRatio: Double = saveDivision(taskMetrics.shuffleReadMetrics.totalBytesRead,
      taskMetrics.shuffleWriteMetrics.bytesWritten)
    val inputOutputRatio: Double = saveDivision(taskMetrics.inputMetrics.bytesRead, taskMetrics.outputMetrics.bytesWritten)
    val memorySpillRatio: Double = saveDivision(taskMetrics.diskBytesSpilled, taskMetrics.peakExecutionMemory)
    StageMetrics(cpuUtilization, gcTimeRatio, shuffleReadWriteRatio, inputOutputRatio, memorySpillRatio)
  }

}

case class StageSubmit(
                        id: Int,
                        name: String,
                        time: Long,
                        scaleOut: Int,
                        parentIds: Array[Int],
                        numTasks: Int,
                      )

case class StageComplete(
                          id: Int,
                          name: String,
                          time: Long,
                          scaleOut: Int,
                          rescalingTimeRation: Double,
                          attemptId: Int,
                          failureReason: String,
                          rddInfo: StageRddInfo,
                          stageMetrics: StageMetrics
                        )

case class StageRddInfo(
                         numPartitions: Int,
                         numCachedPartitions: Int,
                         memSize: Long,
                         diskSize: Long,
                       )
