package com.github.everpeace.kafka

package object reassign_optimizer {

  type ReplicaAssignment = Map[(String, Int), List[Int]]

}
