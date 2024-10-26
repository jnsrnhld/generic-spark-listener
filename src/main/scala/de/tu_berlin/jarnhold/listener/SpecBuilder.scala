package de.tu_berlin.jarnhold.listener

import org.apache.spark.SparkConf
import oshi.SystemInfo
import oshi.hardware.{CentralProcessor, GlobalMemory, HWDiskStore, HardwareAbstractionLayer}
import oshi.software.os.OperatingSystem

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class SpecBuilder(sparkConf: SparkConf) {

  /**
   * HiBench apps usually are submitted with the following naming convention:
   * ALGORITHM_NAME with Params(PARAM1,PARAM2,...)
   */
  private val hibenchAppFromParamsSeparator: String = "with Params\\("

  def buildAppSpecs(): AppSpecs = {
    val (algorithm, params) = sparkConf.get("spark.app.name").split(hibenchAppFromParamsSeparator) match {
      case Array(firstPart, secondPart) =>
        val paramsList = secondPart.stripSuffix(")").split(",").map(_.trim)
        (firstPart, paramsList)
      case _ =>
        (sparkConf.get("spark.app.name"), Array.empty[String])
    }

    AppSpecs(
      algorithm_name = algorithm,
      algorithm_args = params,
      datasize_mb = Math.max(1, sparkConf.getDouble("spark.customExtraListener.datasizeMb", 1.0).toInt),
      target_runtime = sparkConf.get("spark.customExtraListener.targetRuntime").toInt,
      initial_executors = sparkConf.get("spark.dynamicAllocation.initialExecutors").toInt,
      min_executors = sparkConf.get("spark.dynamicAllocation.minExecutors").toInt,
      max_executors = sparkConf.get("spark.dynamicAllocation.maxExecutors").toInt
    )
  }

  def buildDriverSpecs(): DriverSpecs = {
    DriverSpecs(
      cores = sparkConf.get("spark.driver.cores").toInt,
      memory = sparkConf.get("spark.driver.memory"),
      memoryOverhead = sparkConf.get("spark.executor.memoryOverhead")
    )
  }

  def buildExecutorSpecs(): ExecutorSpecs = {
    ExecutorSpecs(
      cores = sparkConf.get("spark.executor.cores").toInt,
      memory = sparkConf.get("spark.executor.memory"),
      memory_overhead = sparkConf.get("spark.driver.memoryOverhead"),
    )
  }

  def buildEnvironmentSpecs(): EnvironmentSpecs = {
    val systemInfo = new SystemInfo()

    val hardware: HardwareAbstractionLayer = systemInfo.getHardware
    val os: OperatingSystem = systemInfo.getOperatingSystem
    val osName: String = os.toString.replaceAll("\\s+", "")

    val processor: CentralProcessor = hardware.getProcessor
    val availableCores: Int = processor.getLogicalProcessorCount

    val diskStores: List[HWDiskStore] = hardware.getDiskStores.asScala.toList
    val totalDiskBytes: Long = diskStores.map(_.getSize).sum
    val totalDiskGb: Double = bytesToGigabytes(totalDiskBytes)

    val memory: GlobalMemory = systemInfo.getHardware.getMemory
    val totalPhysicalMemory: Long = memory.getTotal
    val totalPhysicalMemoryMb: Double = bytesToMegabytes(totalPhysicalMemory)

    val sysInfo = s"os:$osName-cores:$availableCores-memory:$totalPhysicalMemoryMb-disk_size:$totalDiskGb"

    EnvironmentSpecs(
      machine_type = sysInfo,
      hadoop_version = sparkConf.get("spark.customExtraListener.env.hadoopVersion"),
      spark_version = sparkConf.get("spark.customExtraListener.env.sparkVersion"),
      scala_version = sparkConf.get("spark.customExtraListener.env.scalaVersion"),
      java_version = sparkConf.get("spark.customExtraListener.env.javaVersion"),
    )
  }

  private def bytesToMegabytes(bytes: Long): Double = {
    bytes / (1024.0 * 1024)
  }

  private def bytesToGigabytes(bytes: Long): Double = {
    bytes / (1024.0 * 1024 * 1024)
  }

}

// Companion object for static-like properties
object SpecBuilder {
  val requiredSparkConfParams: List[String] = List(
    "spark.driver.cores",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.customExtraListener.datasizeMb",
    "spark.customExtraListener.targetRuntime",
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.customExtraListener.env.hadoopVersion",
    "spark.customExtraListener.env.sparkVersion",
    "spark.customExtraListener.env.scalaVersion",
    "spark.customExtraListener.env.javaVersion"
  )
}
