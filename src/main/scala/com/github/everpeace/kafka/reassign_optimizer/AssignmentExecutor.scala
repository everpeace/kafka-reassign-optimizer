package com.github.everpeace.kafka.reassign_optimizer

import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils

import scala.collection.immutable.ListMap
import scalaz.Scalaz._

case class AssignmentExecutor(zkUtils: ZkUtils) {
  def executeAssignment(assignment: ReplicaAssignment) = {
    println("\n#")
    println("# Executing Reassignment")
    println("#")
    val reassignmentRawJson = zkUtils.formatAsReassignmentJson(
      ListMap(assignment.toSeq.sortBy(_._1): _*)
        .mapKeys(tp => TopicAndPartition(tp._1, tp._2))
    )
    ReassignPartitionsCommand.executeAssignment(zkUtils, reassignmentRawJson)
  }
}

