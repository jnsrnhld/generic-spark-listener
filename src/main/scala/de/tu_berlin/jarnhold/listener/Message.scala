package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.EventType.EventType

sealed trait Message
case class MessageEnvelope[T](event_type: EventType, payload: T) extends Message

case class AppStartMessage(
                              application_id: String,
                              app_name: String,
                              app_time: Long,
                              attempt_id: String,
                              is_adaptive: Boolean,
                              app_specs: AppSpecs,
                              driver_specs: DriverSpecs,
                              executor_specs: ExecutorSpecs,
                              environment_specs: EnvironmentSpecs
                            ) extends Message

case class AppEndMessage(
                              app_event_id: String,
                              app_time: Long,
                              num_executors: Int,
                            ) extends Message

case class JobStartMessage(
                           app_event_id: String,
                           app_time: Long,
                           job_id: Int,
                           num_executors: Int,
                         ) extends Message

case class JobEndMessage(
                            app_event_id: String,
                            app_time: Long,
                            job_id: Int,
                            num_executors: Int,
                            rescaling_time_ratio: Double,
                            stages: Map[String, Stage]
                          ) extends Message


case class ResponseMessage(
                            app_event_id: String,
                            recommended_scale_out: Int
                          ) extends Message

case class StageMetrics(
                         cpu_utilization: Double,
                         gc_time_ratio: Double,
                         shuffle_read_write_ratio: Double,
                         input_output_ratio: Double,
                         memory_spill_ratio: Double
                       )

case class Stage(
                  stage_id: String,
                  stage_name: String,
                  num_tasks: Int,
                  parent_stage_ids: Array[Int],
                  attempt_id: Int,
                  failure_reason: String,
                  start_time: Long,
                  end_time: Long,
                  start_scale_out: Int,
                  end_scale_out: Int,
                  rescaling_time_ratio: Double,
                  rdd_num_partitions: Int,
                  rdd_num_cached_partitions: Int,
                  rdd_mem_size: Long,
                  rdd_disk_size: Long,
                  metrics: StageMetrics,
                )

case class EnvironmentSpecs(
                             machine_type: String,
                             hadoop_version: String,
                             spark_version: String,
                             scala_version: String,
                             java_version: String,
                           )

case class DriverSpecs(
                        cores: Int,
                        memory: String,
                        memory_overhead: String
                      )

case class ExecutorSpecs(
                          cores: Int,
                          memory: String,
                          memory_overhead: String
                        )

case class AppSpecs(
                     algorithm_name: String,
                     algorithm_args: Array[String],
                     datasize_mb: Int,
                     target_runtime: Int,
                     min_executors: Int,
                     max_executors: Int,
                   )
