# kafka-reassign-optimizer.py
This is strongly inspired by [killerwhile/kafka-assignment-optimizer](https://github.com/killerwhile/kafka-assignment-optimizer).

This small python script:

* generates partition replica assignment such that
  * the number of replicas assigned to each broker is balanced, and
  * the number of replica movement is minimum(optimal).
* by formalizing this problem as Mixed Binary Integer Programming
* and the optimization problem is solved by [PuLP](https://pythonhosted.org/PuLP/index.html)
* (optional) you can
  * pin some specific replicas to specific brokers. (see [sample_input_with_pinned_replicas.json](sample_input_with_pinned_replicas.json)).  This would be useful when you don't want to move leader replicas.
  * penalize partition movements by setting your own weights for partitions. (default is `1.0` for all partitions), (see [sample_input_with_penalized_move.json.json](sample_input_with_penalized_move.json))

# How to try?
# on Docker (preferred)

[Docker image](https://hub.docker.com/r/everpeace/kafka-reassign-optimizer/) is available and you can start using this so easily.

```
$ docker pull everpeace/kafka-reassign-optimizer
$ docker run -i -e 'LOGLEVEL=debug' everpeace/kafka-reassign-optimizer < ./sample_input.json
# Configurations for reassignment partition replicas
brokers= [1, 2, 3, 4, 5]
topics= [u'message']
partitions=[(u'message', 3)]
total_partitions= 3
total_replicas_to_assign= 9
balanced_load_min= 1
balanced_load_max= 2

# Binary Integer Programming
Minimize Parition Replica Movement for Partition Reassignment:
MINIMIZE
1*message_0_B4 + 1*message_0_B5 + 1*message_1_B4 + 1*message_1_B5 + 1*message_2_B4 + 1*message_2_B5 + 0
SUBJECT TO
number_or_total_replicas_is_9: message_0_B1 + message_0_B2 + message_0_B3
 + message_0_B4 + message_0_B5 + message_1_B1 + message_1_B2 + message_1_B3
 + message_1_B4 + message_1_B5 + message_2_B1 + message_2_B2 + message_2_B3
 + message_2_B4 + message_2_B5 = 9

partition_(message,0)_has_replication_factor_3: message_0_B1 + message_0_B2
 + message_0_B3 + message_0_B4 + message_0_B5 = 3

partition_(message,1)_has_replication_factor_3: message_1_B1 + message_1_B2
 + message_1_B3 + message_1_B4 + message_1_B5 = 3

partition_(message,2)_has_replication_factor_3: message_2_B1 + message_2_B2
 + message_2_B3 + message_2_B4 + message_2_B5 = 3

load_of_broker_1_is_greater_than_or_equal_to_1: message_0_B1 + message_1_B1
 + message_2_B1 >= 1

load_of_broker_1_is_less_than_or_equal_to_2: message_0_B1 + message_1_B1
 + message_2_B1 <= 2

load_of_broker_2_is_greater_than_or_equal_to_1: message_0_B2 + message_1_B2
 + message_2_B2 >= 1

load_of_broker_2_is_less_than_or_equal_to_2: message_0_B2 + message_1_B2
 + message_2_B2 <= 2

load_of_broker_3_is_greater_than_or_equal_to_1: message_0_B3 + message_1_B3
 + message_2_B3 >= 1

load_of_broker_3_is_less_than_or_equal_to_2: message_0_B3 + message_1_B3
 + message_2_B3 <= 2

load_of_broker_4_is_greater_than_or_equal_to_1: message_0_B4 + message_1_B4
 + message_2_B4 >= 1

load_of_broker_4_is_less_than_or_equal_to_2: message_0_B4 + message_1_B4
 + message_2_B4 <= 2

load_of_broker_5_is_greater_than_or_equal_to_1: message_0_B5 + message_1_B5
 + message_2_B5 >= 1

load_of_broker_5_is_less_than_or_equal_to_2: message_0_B5 + message_1_B5
 + message_2_B5 <= 2

VARIABLES
0 <= message_0_B1 <= 1 Integer
0 <= message_0_B2 <= 1 Integer
0 <= message_0_B3 <= 1 Integer
0 <= message_0_B4 <= 1 Integer
0 <= message_0_B5 <= 1 Integer
0 <= message_1_B1 <= 1 Integer
0 <= message_1_B2 <= 1 Integer
0 <= message_1_B3 <= 1 Integer
0 <= message_1_B4 <= 1 Integer
0 <= message_1_B5 <= 1 Integer
0 <= message_2_B1 <= 1 Integer
0 <= message_2_B2 <= 1 Integer
0 <= message_2_B3 <= 1 Integer
0 <= message_2_B4 <= 1 Integer
0 <= message_2_B5 <= 1 Integer

/usr/local/lib/python2.7/site-packages/pulp/solverdir/cbc/linux/64/cbc /tmp/1-pulp.mps branch printingOptions all solution /tmp/1-pulp.sol

# Result of Optimizing Partition Replica Move
Optimizer Status: Optimal
Number of Parition Replica Movements: 3.0

# CURRENT ASSIGNMENT
broker          1       2       3       4       5
message_0       1       1       1       0       0
message_1       1       1       1       0       0
message_2       1       1       1       0       0


# PROPOSED ASSIGNMENT
broker          1       2       3       4       5
message_0       1       1       0       1       0
message_1       1       1       1       0       0
message_2       0       0       1       1       1


# Partition Replica Assignment (copy it and input to kafka-reassign-partition.sh)
{"vertion": 1, "partitions": [{"topic": "message", "partition": 0, "replicas": [1, 2, 4]}, {"topic": "message", "partition": 1, "replicas": [1, 2, 3]}, {"topic": "message", "partition": 2, "replicas": [3, 4, 5]}]}
```

## on your local machine
you need to install python 2.7.x
```
$ python --version
Python 2.7.12
$ pip install -r requirements.txt
...

$ python kafka-reassign-optimizer.py < sample_input.json
...
```

# Replica assignment as Binary Integer Programming
T.B.D
