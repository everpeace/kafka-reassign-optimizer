package com.github.everpeace.kafka.reassign_optimizer

import kafka.common.TopicAndPartition
import org.scalatest.{FlatSpec, Inside, Matchers}

import scalaz.{Failure, INil, NonEmptyList => NEL}

class TopicPartitionInfoTest extends FlatSpec with Matchers with Inside {
  "TopicPartitionInfo" should "create leader is the preferred replica and all replica is in-sync" in {
    TopicPartitionInfo(
      TopicAndPartition("topic", 0),
      1,
      List(1,2,3),
      List(1,2,3)
    )
  }

  it should "not validated when isr is not subset of replicas" in {
    val validated = TopicPartitionInfo(TopicAndPartition("topic", 0), 1, List(1), List(1,2)).validate

    validated.isFailure shouldBe true
    validated should matchPattern {
      case Failure(NEL(AllReplicaShouldBeISR(_),INil())) =>
    }
  }

  it should "not validated when there exists replica which is not in-sync" in {
    val validated = TopicPartitionInfo(TopicAndPartition("topic", 0), 1, List(1,2,3), List(1,2)).validate

    validated.isFailure shouldBe true
    validated should matchPattern {
      case Failure(NEL(AllReplicaShouldBeISR(_),INil())) =>
    }
  }

  it should "not validated when create leader is NOT the preferred replica" in {
    val validated = TopicPartitionInfo(TopicAndPartition("topic", 0), 1, List(2,1,3), List(1,2,3)).validate

    validated.isFailure shouldBe true
    validated should matchPattern {
      case Failure(NEL(LeaderShouldBePreferredReplica(_),INil())) =>
    }
  }


}
