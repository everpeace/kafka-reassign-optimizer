package com.github.everpeace.kafka.reassign_optimizer

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Solution
import kafka.common.TopicAndPartition
import optimus.algebra.{Const, Constraint, Expression}
import optimus.optimization.SolverLib.SolverLib
import optimus.optimization.{ProblemStatus, _}

/**
  * This class formulates and solve the linear programming of partition reassignment.
  *
  * VARIABLES:
  *   {topic}_{partition}_on_{broker_id} for each topic-partition and broker
  *   1 -> replica of the topic partition is assigned to broker_id
  *   0 -> replica of the topic partition is NOT assigned to broker_id
  *
  * SUBJECT_TO:
  *   minimize total amount of replica movement, which is calculated from given weight.
  *
  * CONSTRAINTS:
  * C0: for each topic-partition, leader doesn't change unless new broker includes original leader.
  *     (This can be done by restricting domain of variable.  [1,1] for pinned replica, [0,1] for free replica.)
  * C1: total replica weight doesn't change.
  * C2: replication factor for each topic-partition doesn't change
  * C3: new assignment is "well balanced"
  *     for each broker, total replica weight assigned to the broker is well balanced.
  *     "well balanced" means that
  *     (balancedFactorMin * idealBalancedWeight) <= (replica weight of the broker) <= (alancedFactorMax * idealBalancedWeight)
  *     because in general perfect balance can't be achieved.
  *
  * @param topicPartitionInfos current topic-partition infos
  * @param targetBrokers       brokers to which topic-partition replicas are distributed
  * @param partitionWeights    weights for each topic-partition
  * @param balancedFactorMin   stretch factor (must be <= 1.0) for deciding solution is balanced. see above for details.
  * @param balancedFactorMax   stretch factor (must be >= 1.0) for deciding solution is balanced. see above for details.
  * @param solverLib           solver lib that will be used.
  */
case class ReassignOptimizationProblem(topicPartitionInfos: List[TopicPartitionInfo],
                                       targetBrokers: Set[Int],
                                       partitionWeights: TopicAndPartition => Int = { _ => 1 },
                                       balancedFactorMin: Double = 0.9,
                                       balancedFactorMax: Double = 1.0
                                      )(implicit val solverLib: SolverLib) {
  require(balancedFactorMin <= 1.0, "balancedFactorMin should be <= 1.0")
  require(balancedFactorMax >= 1.0, "balancedFactorMax should be >= 1.0")
  implicit val `this` = this

  val currentAssignment = topicPartitionInfos.assignment
  val brokersOnProblem = currentAssignment.values.flatten.toSet.union(targetBrokers)

  val totalReplicaWeight = currentAssignment.totalReplicaWeight
  val balancedWeightMin = Math.floor(totalReplicaWeight.toDouble / targetBrokers.size * balancedFactorMin).toInt
  val balancedWeightMax = Math.ceil(totalReplicaWeight.toDouble / targetBrokers.size * balancedFactorMax).toInt

  //
  // problem: it should be mixed-integer problem because variables are boolean(0 or 1)
  //
  implicit val problem: MIProblem = MIProblem(solverLib)

  //
  // variables: it should be Integer var.
  //
  val assignmentVariables: Map[(String, Int, Int), MPIntVar] = for {
    (tp, replicas) <- currentAssignment
    broker <- brokersOnProblem
  } yield {
    val varName = s"${tp._1}_${tp._2}_on_$broker"
    val pinned = 1 to 1
    val free = 0 to 1
    val absent = 0 to 0
    if (!targetBrokers.contains(broker)) {
      (tp._1, tp._2, broker) -> MPIntVar(varName, absent)
    } else if (broker == replicas.head) { // head should be leader
      (tp._1, tp._2, broker) -> MPIntVar(varName, pinned)
    } else {
      (tp._1, tp._2, broker) -> MPIntVar(varName, free)
    }
  }

  //
  // objective function to minimize
  //
  val `total amount of replica movement`: Expression = {
    var move: Expression = Const(0.0)
    for {
      tpb <- assignmentVariables.keys
    } {
      val (t, p, _) = tpb
      if (topicPartitionInfos.tpbPairs.contains(tpb)) {
        move = move + ((assignmentVariables(tpb) - 1) * -1 * partitionWeights((t, p)))
      } else {
        move = move + (assignmentVariables(tpb) * partitionWeights((t, p)))
      }
    }
    move * 0.5
  }

  //
  // constraints
  //
  val `C1: total replica weight doesn't change`: Constraint = {
    var totalReplicaWeights: Expression = Const(0.0)
    for {
      tpb <- assignmentVariables.keys
    } {
      val (t, p, _) = tpb
      totalReplicaWeights = totalReplicaWeights + (assignmentVariables(tpb) * partitionWeights((t, p)))
    }
    totalReplicaWeights := totalReplicaWeight
  }

  val `C2: replication factor for each topic-partition doesn't change`: Map[(String, Int), Constraint] = {
    (for {
      tp <- currentAssignment.keys.toList
    } yield {
      val currentReplicas = currentAssignment(tp).length
      var assignedReplicas: Expression = Const(0.0)
      for {
        b <- brokersOnProblem
      } {
        val (t, p) = tp
        assignedReplicas = assignedReplicas + assignmentVariables((t, p, b))
      }
      tp -> (assignedReplicas := currentReplicas)
    }).toMap
  }

  val `C3: new assignment is well balanced`: Map[Int, (Constraint, Constraint)] = (for {
    b <- brokersOnProblem
  } yield {
    var brokerWeights: Expression = Const(0.0)
    for {
      tp <- currentAssignment.keys.toList
    } {
      val (t, p) = tp
      brokerWeights = brokerWeights + (assignmentVariables((t, p, b)) * partitionWeights(tp))
    }
    if (targetBrokers.contains(b)) {
      b -> (brokerWeights >:= balancedWeightMin, brokerWeights <:= balancedWeightMax)
    } else {
      b -> (brokerWeights >:= 0, brokerWeights <:= 0)
    }
  }).toMap


  def solve(): Solution = {

    minimize(`total amount of replica movement`)
    add(`C1: total replica weight doesn't change`)
    for {
      rfDoesntChange <- `C2: replication factor for each topic-partition doesn't change`.values
    } add(rfDoesntChange)
    for {
      (lower, upper) <- `C3: new assignment is well balanced`.values
    } {
      add(lower)
      add(upper)
    }

    start();
    release()

    new Solution()
  }
}

object ReassignOptimizationProblem {

  class Solution(implicit val problem: ReassignOptimizationProblem) {

    private val mipProblem = problem.problem
    private val assignmentVariables = problem.assignmentVariables
    private val targetBrokers = problem.targetBrokers

    def status: ProblemStatus.ProblemStatus = mipProblem.getStatus

    val originalAssignment: ReplicaAssignment = problem.currentAssignment
    val proposedAssignment: ReplicaAssignment = if (status == ProblemStatus.OPTIMAL) {
      for {
        tpVars <- assignmentVariables.toList.groupBy(a => a._1._1 -> a._1._2)
      } yield {
        val ((t, p), intVars) = tpVars
        val replicas = intVars.flatMap {
          case ((_, _, b), intVar) =>
            intVar.value match {
              case Some(a) if a == 1.0 => List(b)
              case Some(a) if a == 0.0 => List.empty
              case None => List.empty
            }
        }
        val leader = originalAssignment((t, p)).head // head of replicas should be leader.
        if (targetBrokers.contains(leader)) {
          // we have to keep leader being preferred replica.
          (t, p) -> (leader +: replicas.filterNot(_ == leader))
        } else {
          // if previous leader is absent on new broker set
          // we shuffle replica assignment to make new leader will be randomly distributed.
          val shuffled = scala.util.Random.shuffle(replicas)
          println(s"!!WARNING: leader will change in new assignment!! ".toUpperCase + s"${(t, p)}: current=${leader}, new=${shuffled.head}")
          (t, p) -> shuffled
        }
      }
    } else {
      Map.empty
    }


    def showProposedAssignment = proposedAssignment.show

    def moveAmount: Int = if (status == ProblemStatus.OPTIMAL)
      moveAmount(originalAssignment, proposedAssignment)
    else 0

    def brokerWeights = if (status == ProblemStatus.OPTIMAL)
      proposedAssignment.brokerWeights
    else problem.brokersOnProblem.map(b => b -> 0).toMap

    private def moveCount(old: Set[Int], `new`: Set[Int]) = {
      require(old.size == `new`.size)
      // when |old| == |new|  move_cunt = |old - old ∩ new| = |new - old ∩ new|
      old.diff(`new`)
    }

    private def moveAmount(old: ReplicaAssignment, `new`: ReplicaAssignment): Int = (for {
      tp <- old.keys.toList
    } yield {
      moveCount(old(tp).toSet, `new`(tp).toSet).size * problem.partitionWeights(tp)
    }).sum

  }

  object Solution {

    def unapply(solution: Solution): Option[(ProblemStatus.ProblemStatus, Int, Map[Int, Int], ReplicaAssignment)] = {
      val solverStatus = solution.status
      val moveAmount = solution.moveAmount
      val brokerWeights = solution.brokerWeights
      val newAssignment = solution.proposedAssignment
      Some((solverStatus, moveAmount, brokerWeights, newAssignment))
    }

  }

}