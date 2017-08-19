package com.github.everpeace.kafka.reassign_optimizer

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Result
import com.github.everpeace.kafka.reassign_optimizer.util.Tabulator
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
  * @param newBrokers          brokers to which topic-partition replicas are distributed
  * @param partitionWeights    weights for each topic-partition
  * @param balancedFactorMin   stretch factor (must be <= 1.0) for deciding solution is balanced. see above for details.
  * @param balancedFactorMax   stretch factor (must be >= 1.0) for deciding solution is balanced. see above for details.
  * @param solverLib           solver lib that will be used.
  */
case class ReassignOptimizationProblem(topicPartitionInfos: List[TopicPartitionInfo],
                                       newBrokers: Set[Int],
                                       partitionWeights: TopicAndPartition => Int = { _ => 1 },
                                       balancedFactorMin: Double = 0.9,
                                       balancedFactorMax: Double = 1.0
                                      )(implicit val solverLib: SolverLib) {
  require(balancedFactorMin <= 1.0, "balancedFactorMin should be <= 1.0")
  require(balancedFactorMax >= 1.0, "balancedFactorMax should be >= 1.0")

  // to access to weight function
  implicit def tpToTopicAndPartition(tp: (String, Int)): TopicAndPartition = TopicAndPartition(tp._1, tp._2)

  // this map's value is replicas and head of replicas should be leader because converted from TopicPartitionInfos
  val currentReplicaAssignment: ReplicaAssignment = topicPartitionInfos.map(_.assignment).toMap
  val currentAssignmentByTPB: List[(String, Int, Int)] = for {
    tp <- currentReplicaAssignment.keys.toList
    replica <- currentReplicaAssignment(tp)
  } yield (tp._1, tp._2, replica)

  val brokersOnProblem = currentReplicaAssignment.values.flatten.toSet.union(newBrokers)

  val totalReplicaWeight = currentAssignmentByTPB.map(tpb => partitionWeights(tpb._1 -> tpb._2)).sum.toDouble
  val givenBrokerWeights = brokerWeights(currentReplicaAssignment)
  val balancedWeightMin = Math.floor(totalReplicaWeight / newBrokers.size * balancedFactorMin).toInt
  val balancedWeightMax = Math.ceil(totalReplicaWeight / newBrokers.size * balancedFactorMax).toInt

  //
  // problem: it should be mixed-integer problem because variables are boolean(0 or 1)
  //
  implicit val problem: MIProblem = MIProblem(solverLib)

  //
  // variables: it should be Integer var.
  //
  val assignmentVariables: Map[(String, Int, Int), MPIntVar] = for {
    (tp, replicas) <- currentReplicaAssignment
    broker <- brokersOnProblem
  } yield {
    val varName = s"${tp._1}_${tp._2}_on_$broker"
    val pinned = 1 to 1
    val free = 0 to 1
    val absent = 0 to 0
    if (!newBrokers.contains(broker)) {
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
      if (currentAssignmentByTPB.contains(tpb)) {
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
      tp <- currentReplicaAssignment.keys.toList
    } yield {
      val currentReplicas = currentReplicaAssignment(tp).length
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
      tp <- currentReplicaAssignment.keys.toList
    } {
      val (t, p) = tp
      brokerWeights = brokerWeights + (assignmentVariables((t, p, b)) * partitionWeights(tp))
    }
    if (newBrokers.contains(b)) {
      b -> (brokerWeights >:= balancedWeightMin, brokerWeights <:= balancedWeightMax)
    } else {
      b -> (brokerWeights >:= 0, brokerWeights <:= 0)
    }
  }).toMap


  def solve(): Result = {

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

    // transform variables to assignment
    val newAssignment: ReplicaAssignment = if (problem.getStatus == ProblemStatus.OPTIMAL) {
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
        val leader = currentReplicaAssignment((t, p)).head // head of replicas should be leader.
        if (newBrokers.contains(leader)) {
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

    val moves = if (problem.getStatus == ProblemStatus.OPTIMAL)
      moveAmount(currentReplicaAssignment, newAssignment)
    else 0
    val weights = if (problem.getStatus == ProblemStatus.OPTIMAL)
      brokerWeights(newAssignment)
    else newBrokers.map(b => b -> 0).toMap

    Result(
      problem.getStatus,
      moves,
      weights,
      newAssignment
    )
  }

  def showGivenAssignment: String = showAssignment(currentReplicaAssignment)

  def showAssignment(assignments: ReplicaAssignment): String = {
    val sortedBrokers = brokersOnProblem.toList.sorted
    val brokers = List("broker") ++ sortedBrokers.map(_.toString) ++ List("")
    var existsNewLeader = false
    val assignmentRows = for {a <- assignments.keys.toList.sorted} yield {
      val as = sortedBrokers.map { b =>
        if (assignments(a).contains(b)) {
          if (assignments(a).head == b && currentReplicaAssignment(a).head == b) {
            s"⚐${partitionWeights(a)}"
          }
          else if (assignments(a).head == b) {
            existsNewLeader = true
            s"⚑${partitionWeights(a)}"
          }
          else {
            s" ${partitionWeights(a)}"
          }
        } else {
          " 0"
        }
      }
      List(s"[${a._1}, ${a._2}]") ++ as ++ List(s"(RF = ${as.filterNot(_ == " 0").length})")
    }
    val weights = List("weight") ++ {
      val w = brokerWeights(assignments)
      sortedBrokers.map { b =>
        w(b).toString
      }
    } ++ List("")

    val legends = List(
      '⚐' -> "leader partition"
    )

    Tabulator.format(
      List(brokers) ++ assignmentRows ++ List(weights),
      if (existsNewLeader) legends ++ List('⚑' -> "new leader partition") else legends
    )
  }

  def moveAmount(oldP: ReplicaAssignment, newP: ReplicaAssignment): Int = (for {
    tp <- oldP.keys.toList
  } yield {
    oldP(tp).toSet.diff(newP(tp).toSet).size * partitionWeights(tp)
  }).sum

  def brokerWeights(assignment: ReplicaAssignment): Map[Int, Int] = (for {
    b <- brokersOnProblem
  } yield {
    val ws = for {
      tp <- assignment.keys.toList
    } yield {
      if (assignment(tp).contains(b)) partitionWeights(tp)
      else 0
    }
    b -> ws.sum
  }).toMap

}

object ReassignOptimizationProblem {

  case class Result(status: ProblemStatus.ProblemStatus, moveAmount: Int, brokerWeights: Map[Int, Int], newAssignment: ReplicaAssignment)

}