package com.github.everpeace.kafka.reassign_optimizer

import com.github.everpeace.kafka.reassign_optimizer.ReassignOptimizationProblem.Solution

import scala.collection.immutable.ListMap

trait AssignmentPartitioner {
  def partition(solution: Solution): List[ReplicaAssignment]

  def batches(batchAmount: Int, assignments: ReplicaAssignment, moveAmounts: Map[(String, Int), Int]): List[ReplicaAssignment] = {
    if (assignments.isEmpty) {
      Nil
    } else if (moveAmounts(assignments.head._1) > batchAmount){
      val (tp, replicas) = assignments.head
      ListMap(tp -> replicas) +: batches(batchAmount, assignments.tail, moveAmounts)
    } else {
      var w = 0
      val (batch, remained) = assignments.span {
        case (tp, _) =>
          w += moveAmounts(tp)
          if (w <= batchAmount) true
          else false
      }
      batch +: batches(batchAmount, remained, moveAmounts)
    }
  }
}

case class MoveCountPartitioner(batchSize: Int) extends AssignmentPartitioner {
  require(batchSize > 0, s"MoveCountPartitioner: batchSize(=${batchSize}) must be positive.")

  override def partition(solution: Solution): List[ReplicaAssignment] = {
    batches(batchSize, solution.proposedAssignment, solution.moveCounts)
  }
}

case class MoveAmountPartitioner(batchAmount: Int) extends AssignmentPartitioner {
  require(batchAmount > 0, s"MoveAmountPartitioner: batchAmount(=${batchAmount}) must be positive.")

  override def partition(solution: Solution): List[ReplicaAssignment] = {
    batches(batchAmount, solution.proposedAssignment, solution.moveAmounts)
  }
}