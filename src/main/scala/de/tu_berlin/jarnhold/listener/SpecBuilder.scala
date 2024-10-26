package de.tu_berlin.jarnhold.listener

import org.apache.spark.SparkConf

import scala.jdk.CollectionConverters.asScalaIteratorConverter

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
      datasize_mb = sparkConf.get("spark.customExtraListener.datasizeMb").toInt,
      target_runtime = sparkConf.get("spark.customExtraListener.targetRuntime").toInt,
      initial_executors = sparkConf.get("spark.customExtraListener.initialExecutors").toInt,
      min_executors = sparkConf.get("spark.customExtraListener.minExecutors").toInt,
      max_executors = sparkConf.get("spark.customExtraListener.maxExecutors").toInt
    )
  }

  def buildDriverSpecs(): DriverSpecs = {
    DriverSpecs(
      cores = sparkConf.get("spark.customExtraListener.driver.cores").toInt,
      memory = sparkConf.get("spark.customExtraListener.driver.memory"),
      memoryOverhead = Option(sparkConf.get("spark.customExtraListener.driver.memoryOverhead", defaultValue = null))
    )
  }

  def buildExecutorSpecs(): ExecutorSpecs = {
    ExecutorSpecs(
      cores = sparkConf.get("spark.customExtraListener.executor.cores").toInt,
      memory = sparkConf.get("spark.customExtraListener.executor.memory"),
      memoryOverhead = Option(sparkConf.get("spark.customExtraListener.executor.memoryOverhead", defaultValue = null))
    )
  }

  def buildEnvironmentSpecs(): EnvironmentSpecs = {
    val os = System.getProperty("os.name").replaceAll("\\s+", "")
    val cores = Runtime.getRuntime.availableProcessors()
    val memory = java.lang.management.ManagementFactory.getOperatingSystemMXBean match {
      case osBean: com.sun.management.OperatingSystemMXBean =>
        osBean.getTotalMemorySize / (1024 * 1024 * 1024) + "G"
      case _ => "UnknownMem"
    }
    val disk = java.nio.file.FileSystems.getDefault.getFileStores
      .iterator()
      .asScala
      .map(_.getTotalSpace)
      .sum / (1024 * 1024 * 1024) + "G"

    val sysInfo = s"$os-$cores-$memory-$disk"

    EnvironmentSpecs(
      machine_type = sysInfo,
      hadoop_version = sparkConf.get("spark.customExtraListener.env.hadoopVersion"),
      spark_version = sparkConf.get("spark.customExtraListener.env.sparkVersion"),
      scala_version = sparkConf.get("spark.customExtraListener.env.scalaVersion"),
      java_version = sparkConf.get("spark.customExtraListener.env.javaVersion"),
    )
  }
}

// Companion object for static-like properties
object SpecBuilder {
  val requiredSparkConfParams: List[String] = List(
    "spark.customExtraListener.datasizeMb",
    "spark.customExtraListener.targetRuntime",
    "spark.customExtraListener.initialExecutors",
    "spark.customExtraListener.minExecutors",
    "spark.customExtraListener.maxExecutors",
    "spark.customExtraListener.driver.cores",
    "spark.customExtraListener.driver.memory",
    "spark.customExtraListener.driver.memoryOverhead",
    "spark.customExtraListener.executor.cores",
    "spark.customExtraListener.executor.memory",
    "spark.customExtraListener.executor.memoryOverhead",
    "spark.customExtraListener.env.hadoopVersion",
    "spark.customExtraListener.env.sparkVersion",
    "spark.customExtraListener.env.scalaVersion",
    "spark.customExtraListener.env.javaVersion"
  )
}
