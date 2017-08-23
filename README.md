# kafka-reassign-optimizer in scala
[![Build Status](https://travis-ci.org/everpeace/kafka-reassign-optimizer.svg?branch=master)](https://travis-ci.org/everpeace/kafka-reassign-optimizer) [![Docker Pulls](https://img.shields.io/docker/pulls/everpeace/kafka-reassign-optimizer.svg)](https://hub.docker.com/r/everpeace/kafka-reassign-optimizer/)

This is strongly inspired by [killerwhile/kafka-assignment-optimizer](https://github.com/killerwhile/kafka-assignment-optimizer).

This program:

* generates partition replica assignment such that
  * the weight(number) of replicas assigned to each broker is balanced
  * but the weight(number) of replica movement is minimum(optimal)
  * with invariants
    * leader for each topic-partition does not change (unless the leader exists in target brokers).
    * replication factor for each topic-partition does not change.
* by formalizing this problem as Mixed Binary Integer Programming. see [below](#Formulation)
* and the optimization problem is solved by [Optimus](https://github.com/vagmcs/Optimus)
* is already packaged as a docker image [everpeace/kafka-reassign-optimizer](https://hub.docker.com/r/everpeace/kafka-reassign-optimizer/).  it just works by hitting docker command.
* execution and verification of partition reassignment are supported.
* this can be used for both expanding and shrinking case!!

---
<!-- TOC  -->
- [Installation](#installation)
- [How to use](#how-to-use)
- [Sample Scenarios](#sample-scenarios)
	- [Expanding Clusters](#expanding-clusters)
	- [Shringking Clusters](#shringking-clusters)
	- [Executing Reassignment in Batch Mode](#executing-reassignment-in-batch-mode)
	- [Use topic-partition offset as weight](#use-topic-partition-offset-as-weight)
- [Partition Replica Reassignment as Binary Integer Programming](#partition-replica-reassignment-as-binary-integer-programming)
- [How to test locally?](#how-to-test-locally)
- [Release History](#release-history)
<!-- /TOC -->

# Installation
just pull the image
```
$ docker pull everpeace/kafka-reassign-optimizer
```

# How to use
```
$ docker run everpeace/kafka-reassign-optimizer --help
kafka-reassign-optimizer calculating balanced partition replica reassignment but minimum replica move.
Usage: kafka-reassign-optimizer [options]

  -zk, --zookeeper <value>
                           zookeeper connect string (e.g. zk1:2181,zk2:2181/root )
  --brokers id1,id2,...    broker ids to which replicas re distributed to
  --topics topic1,topic2,...
                           target topics (all topics when not specified)
  --weight-type constant|offset
                           type of topic-partition weight. (default = constant)
  --print-assignment       print assignment matrix. please noted this might make huge output when there are a lot topic-partitions (default = false)
  --balanced-factor-min <value>
                           stretch factor to decide new assignment is well-balanced (must be <= 1.0, default = 0.9)
  --balanced-factor-max <value>
                           stretch factor to decide new assignment is well-balanced (must be >= 1.0, default = 1.0)
  -e, --execute            execute re-assignment when found solution is optimal (default = false)
  --batch-weight <value>   execute re-assignment in batch mode (default = 0 (0 means execute re-assignment all at once))
  --verify <value>         verifying reassignment finished after execution of reassignment fired (default = true). this option is active only when execution is on.
  --verify-interval (e.g. 1s, 1min,..)
                           interval duration of verifying reassignment execution progress (default = 5 seconds)
  --verify-timeout (e.g. 1m, 1hour,..)
                           timeout duration of verifying reassignment execution progress (default = 5 minutes)
  --help                   prints this usage text
```

# Sample Scenarios
## Expanding Clusters
- you have 1 topic `tp1`
  - it has 2 partitions with replication factor 3.
  - partition replica is distributed on brokers `1, 2, 3`
- Then, you added two brokers `4,5`
- `kafka-reassign-optimizer` gives you optimal partition replica reassignment such that
  - replica movement is only 2
  - replica partition is almost evenly distributed to 5 brokers
  - leader and replication factor for each topic partition does not change
- and will execute/verify the reassignment.

```
# scenario setup
$ docker-compose up -d zookeeper kafka1 kafka2 kafka3
$ docker-compose exec kafka1 kafka-topics \
  --zookeeper zookeeper:2181 --create --topic tp1 \
  --partitions 2 --replication-factor 3
$ docker-compose up -d kafka4 kafka5
# $ docker-compose down -v # to teardown

# run kafka-reassign-optimizer
$ docker run -it --rm --net host everpeace/kafka-reassign-optimizer --print-assignment --zookeeper 127.0.0.1:2181 --brokers 1,2,3,4,5 -e

#
# Summary of Current Partition Assignment
#
Total Replica Weights: 6.0
Current Broker Set: 1,2,3
Broker Weights: Map(1 -> 2, 2 -> 2, 3 -> 2, 4 -> 0, 5 -> 0)
Current Partition Assignment:

     broker    1    2    3    4    5              

   [tp1, 0]    1    1   ⚐1    0    0   (RF = 3)   
   [tp1, 1]   ⚐1    1    1    0    0   (RF = 3)   

     weight    2    2    2    0    0              
          ⚐   leader partition     

#
# Finding Optimal Partition Assignment
# let's find well balanced but minimum partition movements
#
  ______________________     ______            
  ___  /___  __ \_  ___/________  /__   ______
  __  / __  /_/ /____ \_  __ \_  /__ | / /  _ \
  _  /___  ____/____/ // /_/ /  / __ |/ //  __/
  /_____/_/     /____/ \____//_/  _____/ \___/

Model lpSolve: 13x10
Configuring variable bounds...
Adding objective function...
Creating constraints: Added 13 constraints in 2ms
Solving...
Solution status is Optimal

#
# Summary of Proposed Partition Assignment
#
Is Solution Optimal?: Optimal
Total Replica Weights: 6.0
Replica Move Amount: 2
New Broker Set: 1,2,3,4,5
Broker Weights: Map(1 -> 1, 2 -> 1, 3 -> 2, 4 -> 1, 5 -> 1)
Proposed Partition Assignment:

     broker    1    2    3    4    5              

   [tp1, 0]    0    0   ⚐1    1    1   (RF = 3)   
   [tp1, 1]   ⚐1    1    1    0    0   (RF = 3)   

     weight    1    1    2    1    1              
          ⚐   leader partition     

#
# Proposed Assignment
# (You can save below json and pass it to kafka-reassign-partition command)
#
{
  "version" : 1,
  "partitions" : [
    {
      "topic" : "tp1",
      "partition" : 0,
      "replicas" : [
        3,
        5,
        4
      ]
    },
    {
      "topic" : "tp1",
      "partition" : 1,
      "replicas" : [
        1,
        3,
        2
      ]
    }
  ]
}


Ready to execute above re-assignment?? (y/N)
y

#
# Executing Reassignment
#
Current partition replica assignment

{"version":1,"partitions":[{"topic":"tp1","partition":1,"replicas":[1,3,2]},{"topic":"tp1","partition":0,"replicas":[3,1,2]}]}

Save this to use as the --reassignment-json-file option during rollback
Successfully started reassignment of partitions {"version":1,"partitions":[{"topic":"tp1","partition":0,"replicas":[3,5,4]},{"topic":"tp1","partition":1,"replicas":[1,3,2]}]}

#
# Verifying Reassignment
#  interval = 5 seconds
#  timeout  = 5 minutes (until 2017-08-19T17:10:51.694Z)
#
Verifying.. (time = 2017-08-19T17:05:51.722Z)
Reassignment of partition [tp1,0] is still in progress
Reassignment of partition [tp1,1] is still in progress

Verifying.. (time = 2017-08-19T17:05:56.819Z)
Reassignment of partition [tp1,0] completed successfully
Reassignment of partition [tp1,1] completed successfully

Reassignment execution successfully finished!
```

## Shringking Clusters
- After that, you want to shutdown broker `2, 3`
- Then, you reassgin partition replica to brokers `1,4,5` with `kafka-reassign-optimizer`
- and you will shutdown broker `2,3`

```
$ docker run -it --rm --net host everpeace/kafka-reassign-optimizer --zookeeper localhost:2181 --brokers 1,4,5 --print-assignment -e

#
# Summary of Current Partition Assignment
#
Total Replica Weights: 6.0
Current Broker Set: 1,2,3,4,5
Broker Weights: Map(1 -> 1, 2 -> 1, 3 -> 2, 4 -> 1, 5 -> 1)
Current Partition Assignment:

     broker    1    2    3    4    5              

   [tp1, 0]    0    0   ⚐1    1    1   (RF = 3)   
   [tp1, 1]   ⚐1    1    1    0    0   (RF = 3)   

     weight    1    1    2    1    1              
          ⚐   leader partition     

#
# Finding Optimal Partition Assignment
# let's find well balanced but minimum partition movements
#
  ______________________     ______            
  ___  /___  __ \_  ___/________  /__   ______
  __  / __  /_/ /____ \_  __ \_  /__ | / /  _ \
  _  /___  ____/____/ // /_/ /  / __ |/ //  __/
  /_____/_/     /____/ \____//_/  _____/ \___/

Model lpSolve: 13x10
Configuring variable bounds...
Adding objective function...
Creating constraints: Added 13 constraints in 1ms
Solving...
Solution status is Optimal
!!WARNING: LEADER WILL CHANGE IN NEW ASSIGNMENT!! (tp1,0): current=3, new=4

#
# Summary of Proposed Partition Assignment
#
Is Solution Optimal?: Optimal
Total Replica Weights: 6.0
Replica Move Amount: 3
New Broker Set: 1,4,5
Broker Weights: Map(1 -> 2, 2 -> 0, 3 -> 0, 4 -> 2, 5 -> 2)
Proposed Partition Assignment:

     broker    1    2    3    4    5              

   [tp1, 0]    1    0    0   ⚑1    1   (RF = 3)   
   [tp1, 1]   ⚐1    0    0    1    1   (RF = 3)   

     weight    2    0    0    2    2              
          ⚐   leader partition     
          ⚑   new leader partition   

#
# Proposed Assignment
# (You can save below json and pass it to kafka-reassign-partition command)
#
{
  "version" : 1,
  "partitions" : [
    {
      "topic" : "tp1",
      "partition" : 0,
      "replicas" : [
        4,
        5,
        1
      ]
    },
    {
      "topic" : "tp1",
      "partition" : 1,
      "replicas" : [
        1,
        4,
        5
      ]
    }
  ]
}


Ready to execute above re-assignment?? (y/N)
y

#
# Executing Reassignment
#
Current partition replica assignment

{"version":1,"partitions":[{"topic":"tp1","partition":1,"replicas":[1,3,2]},{"topic":"tp1","partition":0,"replicas":[3,5,4]}]}

Save this to use as the --reassignment-json-file option during rollback
Successfully started reassignment of partitions {"version":1,"partitions":[{"topic":"tp1","partition":0,"replicas":[4,5,1]},{"topic":"tp1","partition":1,"replicas":[1,4,5]}]}

#
# Verifying Reassignment
#  interval = 5 seconds
#  timeout  = 5 minutes (until 2017-08-19T17:22:18.980Z)
#
Verifying.. (time = 2017-08-19T17:17:19.010Z)
Reassignment of partition [tp1,0] is still in progress
Reassignment of partition [tp1,1] is still in progress

Verifying.. (time = 2017-08-19T17:17:24.054Z)
Reassignment of partition [tp1,0] completed successfully
Reassignment of partition [tp1,1] completed successfully

Reassignment execution successfully finished!
```

## Executing Reassignment in Batch Mode
For the case to re-assign bunch of topic-partitions, `kafka-reassign-optimizer` supports batch mode. In batch mode, the tool divide up topic-partitions to move into several batches and execute re-assignment sequentially per batch.

You can control batch size(weight) via `--batch-size`

Below is just an example of 2-batches.
```
 $ docker run -it --rm --net host everpeace/kafka-reassign-optimizer --print-assignment --zookeeper 127.0.0.1:2181 --brokers 1,2,3,4,5 -e --batch-weight 1
...
#
# Reassign Execution in batch mode (batch-weight = 1)
# Proposed assignment was partitioned into 2 batches below:
#   batch 1/2: (tp1,1) (move amount = 1)
#   batch 2/2: (tp1,0) (move amount = 2)

#
# Batch 1/2 = (tp1,1) (move amount = 1)
# Original replica assignment:

#        broker    1    2    3    4    5              
#   
#      [tp1, 1]    1   ⚐1    1    0    0   (RF = 3)   
#   
#             ⚐   leader partition     
# New replica assignment:

#        broker    1    2    3    4    5                                  
#   
#      [tp1, 1]    0   ⚐1    1    0    1   (RF = 3)   (move amount = 1)   
#   
#             ⚐   leader partition                      
#
Ready to execute this batch (y => next batch, s => skip, n => abort)?? (Y/s/n)
y

#
# Executing Reassignment
#
Current partition replica assignment

{"version":1,"partitions":[{"topic":"tp1","partition":2,"replicas":[3,1,2]},{"topic":"tp1","partition":1,"replicas":[2,3,1]},{"topic":"tp1","partition":0,"replicas":[1,2,3]}]}

Save this to use as the --reassignment-json-file option during rollback
Successfully started reassignment of partitions {"version":1,"partitions":[{"topic":"tp1","partition":1,"replicas":[2,3,5]}]}

#
# Verifying Reassignment
#  interval = 5 seconds
#  timeout  = 5 minutes (until 2017-08-22T17:16:04.906Z)
#
1-st try
Verifying.. (time = 2017-08-22T17:11:04.981Z)
Reassignment of partition [tp1,1] is still in progress

2-nd try
Verifying.. (time = 2017-08-22T17:11:09.992Z)
Reassignment of partition [tp1,1] completed successfully

Reassignment execution successfully finished!

#
# Batch 2/2 = (tp1,0) (move amount = 2)
# Original replica assignment:

#        broker    1    2    3    4    5              
#   
#      [tp1, 0]   ⚐1    1    1    0    0   (RF = 3)   
#   
#             ⚐   leader partition     
# New replica assignment:

#        broker    1    2    3    4    5                                  
#   
#      [tp1, 0]   ⚐1    0    0    1    1   (RF = 3)   (move amount = 2)   
#   
#             ⚐   leader partition                      
#
Ready to execute this batch (y => next batch, s => skip, n => abort)?? (Y/s/n)
y

#
# Executing Reassignment
#
Current partition replica assignment

{"version":1,"partitions":[{"topic":"tp1","partition":2,"replicas":[3,1,2]},{"topic":"tp1","partition":1,"replicas":[2,3,5]},{"topic":"tp1","partition":0,"replicas":[1,2,3]}]}

Save this to use as the --reassignment-json-file option during rollback
Successfully started reassignment of partitions {"version":1,"partitions":[{"topic":"tp1","partition":0,"replicas":[1,5,4]}]}

#
# Verifying Reassignment
#  interval = 5 seconds
#  timeout  = 5 minutes (until 2017-08-22T17:16:14.082Z)
#
1-st try
Verifying.. (time = 2017-08-22T17:11:14.083Z)
Reassignment of partition [tp1,0] is still in progress

2-nd try
Verifying.. (time = 2017-08-22T17:11:19.088Z)
Reassignment of partition [tp1,0] completed successfully

Reassignment execution successfully finished!
```

## Use topic-partition offset as weight
By default, weight of topic-partition is just constant(`=1`).  However, there is a case you would like to put weight for each topic-partition by offset values.  Using offsets of topic-paritions can measure more precise amount of data among brokers.

If you want to use this, simply add option `--weight-type offset` like below.  When you use `--weight-type offset`, you might need to adjust `--blanced-factor-{min|max}` because weights of topic-partitions is not uniform.

```
$ docker run -it --rm --net host everpeace/kafka-reassign-optimizer --print-assignment --zookeeper 127.0.0.1:2181 --brokers 1,2,3,4,5 --weight-type offset --balanced-factor-min 0.6 --balanced-factor-max 1.2

#
# Summary of Current Partition Assignment
#
Total Replica Weights: 174
Current Broker Set: 1,2,3
Broker Weights: Map(1 -> 58, 2 -> 58, 3 -> 58, 4 -> 0, 5 -> 0)
Current Partition Assignment:

     broker     1     2     3    4    5              

   [tp1, 0]    20    20   ⚐20    0    0   (RF = 3)   
   [tp1, 1]   ⚐19    19    19    0    0   (RF = 3)   
   [tp1, 2]    19   ⚐19    19    0    0   (RF = 3)   

     weight    58    58    58    0    0              
          ⚐   leader partition        

#
# Finding Optimal Partition Assignment 
# let's find well balanced but minimum partition movements
#
  ______________________     ______            
  ___  /___  __ \_  ___/________  /__   ______ 
  __  / __  /_/ /____ \_  __ \_  /__ | / /  _ \
  _  /___  ____/____/ // /_/ /  / __ |/ //  __/
  /_____/_/     /____/ \____//_/  _____/ \___/ 

Model lpSolve: 14x15
Configuring variable bounds...
Adding objective function...
Creating constraints: Added 14 constraints in 3ms
Solving...
Solution status is Optimal

#
# Summary of Proposed Partition Assignment
#
Is Solution Optimal?: Optimal
Total Replica Weights: 174
Replica Move Amount: 58
New Broker Set: 1,2,3,4,5
Broker Weights: Map(1 -> 39, 2 -> 38, 3 -> 39, 4 -> 38, 5 -> 20)
Proposed Partition Assignment:

     broker     1     2     3     4     5                                   

   [tp1, 0]    20     0   ⚐20     0    20   (RF = 3)   (move amount = 20)   
   [tp1, 1]   ⚐19    19     0    19     0   (RF = 3)   (move amount = 19)   
   [tp1, 2]     0   ⚐19    19    19     0   (RF = 3)   (move amount = 19)   

     weight    39    38    39    38    20                                   
          ⚐   leader partition                            

#
# Proposed Assignment
# (You can save below json and pass it to kafka-reassign-partition command)
#
{
  "version" : 1,
  "partitions" : [
    {
      "topic" : "tp1",
      "partition" : 0,
      "replicas" : [
        3,
        1,
        5
      ]
    },
    {
      "topic" : "tp1",
      "partition" : 1,
      "replicas" : [
        1,
        4,
        2
      ]
    },
    {
      "topic" : "tp1",
      "partition" : 2,
      "replicas" : [
        2,
        4,
        3
      ]
    }
  ]
}
```  

# Partition Replica Reassignment as Binary Integer Programming
VARIABLES:
* `{topic}_{partition}_on_{broker_id}`
  * for each topic-partition, broker
  * it is binary variable (0 or 1).
  * it indicates whether replica of the topic-partition is assigned to the broker or not.

SUBJECT_TO:
  * minimize total amount of replica movement, which is calculated from given weight.
    * calculating total movement amount from above variables is a bit tricky. see [source](src/main/scala/com/github/everpeace/kafka/reassign_optimizer/ReassignOptimizationProblem.scala) for details.

CONSTRAINTS:
  * __C0__: for each topic-partition, leader does not change unless new broker includes original leader.
    * This can be done by restricting domain of variable.
      * `[0,1]` for free spot.
      * `[1,1]` for pinned spot
      * `[0,0]` for prohibited spot.
  * __C1__: total replica weight doesn't change.
  * __C2__: replication factor for each topic-partition does not change
  * __C3__: new assignment is _"well balanced"_
    * for each broker, total replica weight assigned to it is well balanced.
    * "well balanced" means that
      * `(balancedFactorMin * idealBalancedWeight) <= (replica weight of the broker) <= (alancedFactorMax * idealBalancedWeight)`
    * because perfect balance can't be achieved in general.


# How to test locally?
- install lpsolve and jni library by refering [here](https://github.com/vagmcs/Optimus/blob/master/docs/building_and_linking.md#optional-lpsolve-installation)
- assume all needed libraries will be installed at `/usr/local/lib`
- hit `make test`

# Release History
* `0.4.0`:
  - batched re-assignment execution/verification support (#7)
  - support using topic-partition offsets as weights (#8)
* `0.3.0`: initial public release
* `0.2.0`: closed release (heavily experimental)
* `0.1.0`: closed release (heavily experimental)
* originally developed by python (preserved in [this branch](https://github.com/everpeace/kafka-reassign-optimizer/tree/old-python-version))
