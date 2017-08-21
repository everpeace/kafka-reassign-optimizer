package com.github.everpeace.kafka

import kafka.common.TopicAndPartition

package object reassign_optimizer {

  type ReplicaAssignment = Map[(String, Int), List[Int]]

  import scalaz.Isomorphism.<=>
  implicit val isoTopicAndPartitionAndTuple = new (TopicAndPartition <=> (String, Int)) {
    override def to: (TopicAndPartition) => (String, Int) = tp => tp.topic -> tp.partition

    override def from: ((String, Int)) => TopicAndPartition = tp => TopicAndPartition(tp._1, tp._2)
  }
  implicit def tpToTopicAndPartition(tp: (String, Int)): TopicAndPartition = isoTopicAndPartitionAndTuple.from(tp)
  implicit def fromTopicAndPartitionToTuple(tp: TopicAndPartition): (String, Int) = isoTopicAndPartitionAndTuple.to(tp)
}
