package com.github.everpeace.kafka.reassign_optimizer

import kafka.common.{TopicAndPartition => TP}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse

import scala.collection.convert.decorateAsScala._
import scalaz.Scalaz._
import scalaz._

case class TopicPartitionInfo(tp: TP, leader: Int, replicas: List[Int], isrs: List[Int]) {

  import TopicPartitionInfo.Validated

  private def validateBalanced: Validated = this.success
    .ensure(LeaderShouldBePreferredReplica(this).asInstanceOf[RuntimeException]) { tpi =>
      tpi.leader == tpi.replicas.head
    }.toValidationNel

  private def validateIsr: Validated = this.success
    .ensure(AllReplicaShouldBeISR(this).asInstanceOf[RuntimeException]) { tpi =>
      tpi.replicas.toSet == tpi.isrs.toSet
    }.toValidationNel

  def validate: Validated
  = (validateIsr |@| validateBalanced) ((_, _) => this)
}

sealed class TopicPartitionInfoCreationException(msg: String) extends RuntimeException(msg)

case class LeaderShouldBePreferredReplica(tpi: TopicPartitionInfo)
  extends TopicPartitionInfoCreationException(s"Leader should be the preferred replica (${tpi})")

case class AllReplicaShouldBeISR(tpi: TopicPartitionInfo)
  extends TopicPartitionInfoCreationException(s"All replica should be in ISR (${tpi})")

object TopicPartitionInfo {
  type Validated = ValidationNel[RuntimeException, TopicPartitionInfo]

  def apply(topic: String, partitionMetadata: MetadataResponse.PartitionMetadata): Validated = {
    if (partitionMetadata.error() != Errors.NONE) {
      partitionMetadata.error().exception().asInstanceOf[RuntimeException].failureNel
    } else {
      TopicPartitionInfo(
        TP(topic, partitionMetadata.partition()),
        partitionMetadata.leader().id(),
        partitionMetadata.replicas().asScala.toList.map(_.id()),
        partitionMetadata.isr().asScala.toList.map(_.id())
      ).validate
    }
  }
}
