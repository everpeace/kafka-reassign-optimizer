package com.github.everpeace.kafka.reassign_optimizer

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Result
import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import optimus.optimization.{ProblemStatus, SolverLib}
import org.apache.kafka.common.security.JaasUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.immutable.ListMap
import scala.collection.{Map, Seq}
import scala.util.control.Breaks
import scalaz.Scalaz._
import scalaz._

object Main extends App {
  val configOpt = Config.parser.parse(args, Config())
  if (configOpt.isEmpty) sys.exit(1)

  val config = configOpt.get
  import config._

  val zk = ZkUtils(zkString, 30000, 30000, JaasUtils.isZkSecurityEnabled)
  val partitionWeights = (_: TopicAndPartition) => 1
  val topicMetas = AdminUtils.fetchTopicMetadataFromZk(
    if (topics.isEmpty) zk.getAllTopics().toSet else topics,
    zk
  )

  val tpisValidated: Validation[NonEmptyList[RuntimeException], List[TopicPartitionInfo]] = (for {
    t <- topicMetas.toList
    p <- t.partitionMetadata().asScala.toList
  } yield TopicPartitionInfo(t.topic(), p))
    .sequence[({type λ[T] = ValidationNel[RuntimeException, T]})#λ, TopicPartitionInfo]
    .map(_.groupBy(_.tp.topic).mapValues(_.sortBy(_.tp.partition))).map(_.values.flatten.toList)

  tpisValidated match {
    case Failure(e) =>
      println("\n#")
      println("# !!Several Failure Happens!!".toUpperCase)
      e.foreach(println)
      sys.exit(1)

    case Success(tpis) =>

      implicit val lp_solve = SolverLib.lp_solve
      val problem = ReassignOptimizationProblem(
        topicPartitionInfos = tpis,
        newBrokers = newBrokers,
        partitionWeights = partitionWeights,
        balancedFactorMin = balancedFactorMin,
        balancedFactorMax = balancedFactorMax
      )

      println("\n#")
      println("# Summary of Current Partition Assignment")
      println("#")
      println(s"Total Replica Weights: ${problem.totalReplicaWeight}")
      println(s"Current Broker Set: ${problem.currentReplicaAssignment.values.flatten.toSet.toList.sorted.mkString(",")}")
      println(s"Broker Weights: ${ListMap(problem.givenBrokerWeights.toSeq.sortBy(_._1): _*)}")
      if (printAssignment)
        println("Current Partition Assignment:")
      println(problem.showGivenAssignment)

      println("\n#")
      println("# Finding Optimal Partition Assignment ")
      println("# let's find well balanced but minimum partition movements")
      println("#")
      val Result(solverStatus, moveAmount, brokerWeights, newAssignment) = problem.solve()

      println("\n#")
      println("# Summary of Proposed Partition Assignment")
      println("#")
      println(s"Is Solution Optimal?: ${solverStatus}")
      if (solverStatus != ProblemStatus.OPTIMAL) {
        println(s"!!WARNING: Solution is ${solverStatus}. DON'T execute below assignment plan!!!".toUpperCase)
      }
      println(s"Total Replica Weights: ${problem.totalReplicaWeight}")
      println(s"Replica Move Amount: ${moveAmount}")
      println(s"New Broker Set: ${newBrokers.toList.sorted.mkString(",")}")
      println(s"Broker Weights: ${ListMap(brokerWeights.toSeq.sortBy(_._1): _*)}")
      if (printAssignment)
        println("Proposed Partition Assignment:")
      println(problem.showAssignment(newAssignment))


      println("\n#")
      println("# Proposed Assignment")
      println("# (You can save below json and pass it to kafka-reassign-partition command)")
      println("#")
      val reassignmentRawJson = zk.formatAsReassignmentJson(
        ListMap(newAssignment.toSeq.sortBy(_._1): _*)
          .mapKeys(tp => TopicAndPartition(tp._1, tp._2))
      )

      import io.circe.parser._
      parse(reassignmentRawJson) match {
        case Left(e) => println(e)
        case Right(j) => println(j.spaces2)
      }

      if (execute) {
        println("\n")
        println("Ready to execute above re-assignment?? (y/N)")
        scala.io.StdIn.readBoolean match {
          case false =>
            println("aborted.")
          case true =>
            println("\n#")
            println("# Executing Reassignment")
            println("#")
            ReassignPartitionsCommand.executeAssignment(zk, reassignmentRawJson)

            if (verify) {
              val start = Instant.now
              println("\n#")
              println("# Verifying Reassignment")
              println(s"#  interval = ${verifyInterval} ")
              println(s"#  timeout  = ${verifyTimeout} (until ${start.plus(verifyTimeout.toMillis, ChronoUnit.MILLIS)})")
              println("#")
              val b = new Breaks
              var status: ReassignmentStatus = ReassignmentInProgress
              b.breakable {
                while (Instant.now.isBefore(start.plus(verifyTimeout.toMillis, ChronoUnit.MILLIS))) {
                  val st = verifyAssignment(zk, reassignmentRawJson)
                  status = st
                  if (status != ReassignmentInProgress) {
                    b.break
                  }
                  Thread.sleep(verifyInterval.toMillis)
                }
                verifyAssignment(zk, reassignmentRawJson)
                println("Verifying reassignments timeout! Reassignments might be in progress. Check manually.")
                sys.exit(0)
              }
              status match {
                case ReassignmentCompleted =>
                  println("Reassignment execution successfully finished!")
                case ReassignmentFailed =>
                  println("Reassignment execution finished! Several reassignments might be failed. Check manually.")
              }
            } else {
              println("Reassignment execution finished! Check reassignment progress manually.")
            }
        }
      }
  }

  def verifyAssignment(zkUtils: ZkUtils, reassignmentJsonString: String): ReassignmentStatus = {
    println(s"Verifying.. (time = ${Instant.now})")
    val partitionsToBeReassigned = zkUtils.parsePartitionReassignmentData(reassignmentJsonString)
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
    if (reassignedPartitionsStatus.values.forall(_ == ReassignmentCompleted)){
      ReassignmentCompleted
    } else if (reassignedPartitionsStatus.values.exists(_ == ReassignmentInProgress)){
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

