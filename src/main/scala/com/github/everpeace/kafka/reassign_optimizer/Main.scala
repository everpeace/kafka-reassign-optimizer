package com.github.everpeace.kafka.reassign_optimizer

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Solution
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
      implicit val problem = ReassignOptimizationProblem(
        topicPartitionInfos = tpis,
        targetBrokers = newBrokers,
        partitionWeights = partitionWeights,
        balancedFactorMin = balancedFactorMin,
        balancedFactorMax = balancedFactorMax
      )
      val currentAssignment = tpis.assignment

      println("\n#")
      println("# Summary of Current Partition Assignment")
      println("#")
      println(s"Total Replica Weights: ${problem.totalReplicaWeight}")
      println(s"Current Broker Set: ${currentAssignment.values.flatten.toSet.toList.sorted.mkString(",")}")
      println(s"Broker Weights: ${ListMap(currentAssignment.brokerWeights.toSeq.sortBy(_._1): _*)}")
      if (printAssignment)
        println("Current Partition Assignment:")
      println(currentAssignment.show)

      println("\n#")
      println("# Finding Optimal Partition Assignment ")
      println("# let's find well balanced but minimum partition movements")
      println("#")

      val Solution(solverStatus, moveAmount, brokerWeights, proposedAssignment) = problem.solve()

      println("\n#")
      println("# Summary of Proposed Partition Assignment")
      println("#")
      println(s"Is Solution Optimal?: ${solverStatus}")
      println(s"Total Replica Weights: ${problem.totalReplicaWeight}")
      println(s"Replica Move Amount: ${moveAmount}")
      println(s"New Broker Set: ${newBrokers.toList.sorted.mkString(",")}")
      println(s"Broker Weights: ${ListMap(brokerWeights.toSeq.sortBy(_._1): _*)}")
      if (printAssignment)
        println("Proposed Partition Assignment:")
      println(proposedAssignment.show)

      if (solverStatus != ProblemStatus.OPTIMAL) {
        println(s"!!Solution is ${solverStatus}. Aborted!!".toUpperCase)
        sys.exit(0)
      }

      println("\n#")
      println("# Proposed Assignment")
      println("# (You can save below json and pass it to kafka-reassign-partition command)")
      println("#")
      val reassignmentRawJson = zk.formatAsReassignmentJson(
        ListMap(proposedAssignment.toSeq.sortBy(_._1): _*)
          .mapKeys(tp => TopicAndPartition(tp._1, tp._2))
      )

      import io.circe.parser._
      parse(reassignmentRawJson) match {
        case Left(e) =>
          println(e)
          sys.exit(1)
        case Right(j) => println(j.spaces2)
      }

      if (execute) {
        println("\n")
        println("Ready to execute above re-assignment?? (y/N)")
        scala.io.StdIn.readBoolean match {
          case false =>
            println("aborted.")
          case true =>

            OneShotAssignmentExecutor(zk).executeAssignment(proposedAssignment)

            if (verify) {
              val status = PollingAssignmentVerifyier(zk, verifyInterval, verifyTimeout)
                .verify(proposedAssignment)

              status match {
                case ReassignmentCompleted =>
                  println("Reassignment execution successfully finished!")
                case ReassignmentInProgress =>
                  println("Verifying reassignments timeout! Reassignments might be in progress. Check manually.")
                case ReassignmentFailed =>
                  println("Reassignment execution finished! Several reassignments might be failed. Check manually.")
              }

            } else {
              println("Reassignment execution finished! Check reassignment progress manually.")
            }
        }
      }
  }
}

