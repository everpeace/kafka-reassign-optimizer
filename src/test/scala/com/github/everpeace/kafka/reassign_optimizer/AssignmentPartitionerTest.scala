package com.github.everpeace.kafka.reassign_optimizer

import org.scalatest.{FlatSpec, Matchers}

class AssignmentPartitionerTest extends FlatSpec with Matchers {

  "MoveCountPartitioner" should "not be able to instantiate with batchSize <= 0" in {
    var thrown = the[IllegalArgumentException] thrownBy MoveCountPartitioner(0)
    thrown.getMessage should include("MoveCountPartitioner: batchSize(=0) must be positive")

    thrown = the[IllegalArgumentException] thrownBy MoveCountPartitioner(-1)
    thrown.getMessage should include("MoveCountPartitioner: batchSize(=-1) must be positive")
  }

  "MoveAmountPartitioner" should "not be able to instantiate with batchSize <= 0" in {
    var thrown = the[IllegalArgumentException] thrownBy MoveAmountPartitioner(0)
    thrown.getMessage should include("MoveAmountPartitioner: batchAmount(=0) must be positive")

    thrown = the[IllegalArgumentException] thrownBy MoveAmountPartitioner(-1)
    thrown.getMessage should include("MoveAmountPartitioner: batchAmount(=-1) must be positive")
  }

  val batchesTestee = new AssignmentPartitioner {
    override def partition(solution: ReassignOptimizationProblem.Solution): List[ReplicaAssignment] = ???
  }
  "AssignmentPartitioner.batches" should "work when assignment is Empty" in {
    batchesTestee.batches(10, Map.empty, Map.empty) shouldBe Nil
  }

  it should "work when head' amount is > batchAmount" in {
    batchesTestee.batches(1, Map(("t",0) -> List(1)), Map(("t",0) -> 2)) shouldBe List(Map(("t",0)->List(1)))
  }

  it should "work when head's amount is <= batchAmount" in {
    batchesTestee.batches(
      2,
      Map(("t",0) -> List(1), ("t",1) -> List(1), ("t",2) -> List(1)),
      Map(("t",0) -> 2      , ("t",1) -> 1      , ("t",2) -> 1      )
    ) shouldBe List(
        Map(("t",0)->List(1)),
        Map(("t",1) -> List(1), ("t",2) -> List(1))
    )
  }

}
