package com.github.everpeace.kafka.reassign_optimizer

import java.time.Instant
import java.time.temporal.ChronoUnit

import kafka.admin.{ReassignmentCompleted, ReassignmentFailed, ReassignmentInProgress, ReassignmentStatus}
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils

import scala.collection.immutable.ListMap
import scala.collection.{Map, Seq}
import scala.concurrent.duration.Duration
import scala.util.control.Breaks
import scalaz.Scalaz._

trait AssignmentVerifier {
  val zkUtils: ZkUtils

  def verify(assignment: ReplicaAssignment): ReassignmentStatus

  implicit def mapKeyToTopicAndPartition(ra: ReplicaAssignment) = ListMap(ra.toSeq.sortBy(_._1): _*)
    .mapKeys(tp => TopicAndPartition(tp._1, tp._2))

  def verifyAssignment(partitionsToBeReassigned: ReplicaAssignment): ReassignmentStatus = {
    println(s"Verifying.. (time = ${Instant.now})")
    val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkUtils, partitionsToBeReassigned)
    reassignedPartitionsStatus.foreach { partition =>
      partition._2 match {
        case ReassignmentCompleted =>
          println("Reassignment of partition %s completed successfully".format(partition._1))
        case ReassignmentFailed =>
          println("Reassignment of partition %s failed".format(partition._1))
        case ReassignmentInProgress =>
          println("Reassignment of partition %s is still in progress".format(partition._1))
      }
    }
    println()
    if (reassignedPartitionsStatus.values.forall(_ == ReassignmentCompleted)) {
      ReassignmentCompleted
    } else if (reassignedPartitionsStatus.values.exists(_ == ReassignmentInProgress)) {
      ReassignmentInProgress
    } else {
      ReassignmentFailed
    }
  }

  private def checkIfReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  : Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition._1,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
        if (assignedReplicas == newReplicas)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }
}

case class PollingAssignmentVerifyier(zkUtils: ZkUtils, interval: Duration, timeout: Duration) extends AssignmentVerifier {
  override def verify(assignment: ReplicaAssignment): ReassignmentStatus = {
    val start = Instant.now
    println("\n#")
    println("# Verifying Reassignment")
    println(s"#  interval = ${interval} ")
    println(s"#  timeout  = ${timeout} (until ${start.plus(timeout.toMillis, ChronoUnit.MILLIS)})")
    println("#")
    val b = new Breaks
    var status: ReassignmentStatus = ReassignmentInProgress
    b.breakable {
      var i = 1
      def suff(c: Int) = if (c % 10 == 1) "st" else if (c % 10 == 2) "nd" else if (c % 10 == 3) "rd" else "th"
      while (Instant.now.isBefore(start.plus(timeout.toMillis, ChronoUnit.MILLIS))) {
        println(s"$i-${suff(i)} try")
        val st = verifyAssignment(assignment)
        i += 1
        status = st
        if (status != ReassignmentInProgress) {
          b.break
        }
        Thread.sleep(interval.toMillis)
      }
      println(s"$i-${suff(i)} try")
      verifyAssignment(assignment)
    }
    status
  }
}
