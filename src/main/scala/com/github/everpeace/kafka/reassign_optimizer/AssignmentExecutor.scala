package com.github.everpeace.kafka.reassign_optimizer

import java.time.Instant

import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils

import scala.collection.{Map, Seq}
import scala.collection.immutable.ListMap
import scalaz.Scalaz._

trait AssignmentExecutor {
  val zkUtils: ZkUtils
  def executeAssignment(assignment: ReplicaAssignment)
}

case class OneShotAssignmentExecutor(zkUtils: ZkUtils) extends AssignmentExecutor {
  def executeAssignment(assignment: ReplicaAssignment) = {
    println("\n#")
    println("# Executing Reassignment in one shot")
    println("#")
    val reassignmentRawJson = zkUtils.formatAsReassignmentJson(
      ListMap(assignment.toSeq.sortBy(_._1): _*)
        .mapKeys(tp => TopicAndPartition(tp._1, tp._2))
    )
    ReassignPartitionsCommand.executeAssignment(zkUtils, reassignmentRawJson)
  }
}

