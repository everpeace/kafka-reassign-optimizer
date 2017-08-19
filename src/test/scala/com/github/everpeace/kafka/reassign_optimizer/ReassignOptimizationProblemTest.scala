package com.github.everpeace.kafka.reassign_optimizer

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Result
import kafka.common.TopicAndPartition
import optimus.optimization.{ProblemStatus, SolverLib}
import org.scalatest.{FlatSpec, Matchers}

class ReassignOptimizationProblemTest extends FlatSpec with Matchers {

  def validate(tpis: TopicPartitionInfo*): List[TopicPartitionInfo] = {
    import scalaz._, Scalaz._
    val validated = tpis.toList.map(_.validate)
      .sequence[({type λ[T] = ValidationNel[RuntimeException, T]})#λ, TopicPartitionInfo]

    validated match {
      case Success(_) => tpis.toList
      case Failure(e) => throw new RuntimeException(s"${e}")
    }
  }

  "ReassignOptimizationProblem" should "be solved when expanding cluster" in {
    // replicated partition among 3 nodes
    val current = validate(
      TopicPartitionInfo(TopicAndPartition("message", 0), 1, List(1,2,3), List(1,2,3)),
      TopicPartitionInfo(TopicAndPartition("message", 1), 2, List(2,3,1), List(1,2,3))
    )

    // expanding to 6 nodes
    val newBrokers = Set(1,2,3,4,5,6)

    implicit val lp_solve = SolverLib.lp_solve
    val problem = ReassignOptimizationProblem(current, newBrokers)

    println("Current Partition Assignment:")
    println(problem.showGivenAssignment)

    val Result(solverStatus, moveAmount, brokerWeights, newAssignment) = problem.solve()

    println(s"Partition Move Amount: ${moveAmount}")
    println("Propoesd Partition Assignment:")
    println(problem.showAssignment(newAssignment))

    solverStatus shouldBe ProblemStatus.OPTIMAL
    moveAmount shouldBe 3.0
    brokerWeights shouldBe Map(1 -> 1.0, 2 -> 1.0, 3 -> 1.0, 4 -> 1.0, 5 -> 1.0, 6 -> 1.0)
    newAssignment("message" -> 0).head shouldBe 1
    newAssignment("message" -> 1).head shouldBe 2
  }

  it should "be solved when shrinking cluster with keeping current leaders" in {
    // replicated partition among 5 nodes
    val current = validate(
      TopicPartitionInfo(TopicAndPartition("message", 0), 2, List(2,5,4), List(2,5,4)),
      TopicPartitionInfo(TopicAndPartition("message", 1), 3, List(3,1,2), List(3,1,2))
    )

    // shrinking to 3 nodes
    val newBrokers = Set(1,2,3)

    implicit val lp_solve = SolverLib.lp_solve
    val problem = ReassignOptimizationProblem(current, newBrokers)

    println("Current Partition Assignment:")
    println(problem.showAssignment(current.map(_.assignment).toMap))

    val Result(solverStatus, moveAmount, brokerWeights, newAssignment) = problem.solve()

    println(s"Partition Move Amount: ${moveAmount}")
    println("Propoesd Partition Assignment:")
    println(problem.showAssignment(newAssignment))

    solverStatus shouldBe ProblemStatus.OPTIMAL
    moveAmount shouldBe 2.0
    brokerWeights shouldBe Map(1 -> 2.0, 2 -> 2.0, 3 -> 2.0, 4 -> 0.0, 5 -> 0.0)
    newAssignment("message" -> 0).head shouldBe 2
    newAssignment("message" -> 1).head shouldBe 3
  }

  it should "be solved when shrinking cluster without keeping current leaders" in {
    // replicated partition among 5 nodes
    val current = validate(
      TopicPartitionInfo(TopicAndPartition("message", 0), 2, List(2,5,4), List(2,5,4)),
      TopicPartitionInfo(TopicAndPartition("message", 1), 3, List(3,1,2), List(3,1,2))
    )

    // shrinking to 3 nodes (current leaders(2,3) will be vanished)
    val newBrokers = Set(1,4,5)

    implicit val lp_solve = SolverLib.lp_solve
    val problem = ReassignOptimizationProblem(current, newBrokers)

    println("Current Partition Assignment:")
    println(problem.showAssignment(current.map(_.assignment).toMap))

    val Result(solverStatus, moveAmount, brokerWeights, newAssignment) = problem.solve()

    println(s"Partition Move Amount: ${moveAmount}")
    println("Propoesd Partition Assignment:")

    solverStatus shouldBe ProblemStatus.OPTIMAL
    moveAmount shouldBe 3.0
    println(problem.showAssignment(newAssignment))
    brokerWeights shouldBe Map(1 -> 2.0, 2 -> 0.0, 3 -> 0.0, 4 -> 2.0, 5 -> 2.0)
  }

  it should "be infeasible when new broker set is less than original replication factor" in {
    // replicated partition among 3 nodes
    val current = validate(
      TopicPartitionInfo(TopicAndPartition("message", 0), 1, List(1,2,3), List(1,2,3)),
      TopicPartitionInfo(TopicAndPartition("message", 1), 2, List(2,3,1), List(1,2,3))
    )

    // expanding to 2 nodes
    val newBrokers = Set(1,2)

    implicit val lp_solve = SolverLib.lp_solve
    val problem = ReassignOptimizationProblem(current, newBrokers)

    println("Current Partition Assignment:")
    println(problem.showGivenAssignment)

    val Result(solverStatus, moveAmount, brokerWeights, newAssignment) = problem.solve()

    println(s"Partition Move Amount: ${moveAmount}")
    println("Propoesd Partition Assignment:")
    println(problem.showAssignment(newAssignment))

    solverStatus shouldBe ProblemStatus.INFEASIBLE
  }
}
