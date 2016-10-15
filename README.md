# kafka-reassign-optimizer.py
This is strongly inspired by [killerwhile/kafka-assignment-optimizer](https://github.com/killerwhile/kafka-assignment-optimizer).

This small python script:

* generates partition replica assignment such that
  * the number of replicas assigned to each broker is balanced, and
  * the number of replica movement is minimum(optimal).
* by formalizing this problem as Mixed Binary Integer Programming
* and the optimization problem is solved by [PuLP](https://pythonhosted.org/PuLP/index.html)

# How to try?
# on Docker (preferred)

[Docker image](https://hub.docker.com/r/everpeace/kafka-reassign-optimizer/) is available and you can start using this so easily.

```
$ docker pull everpeace/kafka-reassign-optimizer
$ docker run -i -e 'LOGLEVEL=debug' everpeace/kafka-reassign-optimizer < ./sample_input.json
# input information for making assignment proposal
brokers= [1, 2, 3, 4, 5]
new_replication_factor= -1
topics= [u'message']
partitions=[(u'message', 8)]
total_partitions= 8
total_replicas_to_assign= 24
balanced_load_min= 4
balanced_load_max= 5

# Binary Integer Programming
Minimize Parition Replica Movement for Partition Reassignment:
MINIMIZE
1*message_0_B4 + 1*message_0_B5 + 1*message_1_B4 + 1*message_1_B5 + 1*message_2_B4 + 1*message_2_B5 + 1*message_3_B4 + 1*message_3_B5 + 1*message_4_B4 + 1*message_4_B5 + 1*message_5_B4 + 1*message_5_B5 + 1*message_6_B4 + 1*message_6_B5 +
1*message_7_B4 + 1*message_7_B5 + 0
SUBJECT TO
number_or_total_replicas_is_24: message_0_B1 + message_0_B2 + message_0_B3
 + message_0_B4 + message_0_B5 + message_1_B1 + message_1_B2 + message_1_B3
 + message_1_B4 + message_1_B5 + message_2_B1 + message_2_B2 + message_2_B3
 + message_2_B4 + message_2_B5 + message_3_B1 + message_3_B2 + message_3_B3
 + message_3_B4 + message_3_B5 + message_4_B1 + message_4_B2 + message_4_B3
 + message_4_B4 + message_4_B5 + message_5_B1 + message_5_B2 + message_5_B3
 + message_5_B4 + message_5_B5 + message_6_B1 + message_6_B2 + message_6_B3
 + message_6_B4 + message_6_B5 + message_7_B1 + message_7_B2 + message_7_B3
 + message_7_B4 + message_7_B5 = 24


partition_(message,0)_has_replication_factor_3: message_0_B1 + message_0_B2
 + message_0_B3 + message_0_B4 + message_0_B5 = 3

partition_(message,1)_has_replication_factor_3: message_1_B1 + message_1_B2
 + message_1_B3 + message_1_B4 + message_1_B5 = 3

partition_(message,2)_has_replication_factor_3: message_2_B1 + message_2_B2
 + message_2_B3 + message_2_B4 + message_2_B5 = 3

partition_(message,3)_has_replication_factor_3: message_3_B1 + message_3_B2
 + message_3_B3 + message_3_B4 + message_3_B5 = 3

partition_(message,4)_has_replication_factor_3: message_4_B1 + message_4_B2
 + message_4_B3 + message_4_B4 + message_4_B5 = 3

partition_(message,5)_has_replication_factor_3: message_5_B1 + message_5_B2
 + message_5_B3 + message_5_B4 + message_5_B5 = 3

partition_(message,6)_has_replication_factor_3: message_6_B1 + message_6_B2
 + message_6_B3 + message_6_B4 + message_6_B5 = 3

partition_(message,7)_has_replication_factor_3: message_7_B1 + message_7_B2
 + message_7_B3 + message_7_B4 + message_7_B5 = 3

load_of_broker_1_is_greater_than_or_equal_to_4: message_0_B1 + message_1_B1
 + message_2_B1 + message_3_B1 + message_4_B1 + message_5_B1 + message_6_B1
 + message_7_B1 >= 4

load_of_broker_1_is_less_than_or_equal_to_5: message_0_B1 + message_1_B1
 + message_2_B1 + message_3_B1 + message_4_B1 + message_5_B1 + message_6_B1
 + message_7_B1 <= 5

load_of_broker_2_is_greater_than_or_equal_to_4: message_0_B2 + message_1_B2
 + message_2_B2 + message_3_B2 + message_4_B2 + message_5_B2 + message_6_B2
 + message_7_B2 >= 4

load_of_broker_2_is_less_than_or_equal_to_5: message_0_B2 + message_1_B2
 + message_2_B2 + message_3_B2 + message_4_B2 + message_5_B2 + message_6_B2
 + message_7_B2 <= 5

load_of_broker_3_is_greater_than_or_equal_to_4: message_0_B3 + message_1_B3
 + message_2_B3 + message_3_B3 + message_4_B3 + message_5_B3 + message_6_B3
 + message_7_B3 >= 4

load_of_broker_3_is_less_than_or_equal_to_5: message_0_B3 + message_1_B3
 + message_2_B3 + message_3_B3 + message_4_B3 + message_5_B3 + message_6_B3
 + message_7_B3 <= 5

load_of_broker_4_is_greater_than_or_equal_to_4: message_0_B4 + message_1_B4
 + message_2_B4 + message_3_B4 + message_4_B4 + message_5_B4 + message_6_B4
 + message_7_B4 >= 4

load_of_broker_4_is_less_than_or_equal_to_5: message_0_B4 + message_1_B4
 + message_2_B4 + message_3_B4 + message_4_B4 + message_5_B4 + message_6_B4
 + message_7_B4 <= 5

load_of_broker_5_is_greater_than_or_equal_to_4: message_0_B5 + message_1_B5
 + message_2_B5 + message_3_B5 + message_4_B5 + message_5_B5 + message_6_B5
 + message_7_B5 >= 4

load_of_broker_5_is_less_than_or_equal_to_5: message_0_B5 + message_1_B5
 + message_2_B5 + message_3_B5 + message_4_B5 + message_5_B5 + message_6_B5
 + message_7_B5 <= 5

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
0 <= message_1_B5 <= 1 Integer
0 <= message_2_B1 <= 1 Integer
0 <= message_2_B2 <= 1 Integer
0 <= message_2_B3 <= 1 Integer
0 <= message_2_B4 <= 1 Integer
0 <= message_2_B5 <= 1 Integer
0 <= message_3_B1 <= 1 Integer
0 <= message_3_B2 <= 1 Integer
0 <= message_3_B3 <= 1 Integer
0 <= message_3_B4 <= 1 Integer
0 <= message_3_B5 <= 1 Integer
0 <= message_4_B1 <= 1 Integer
0 <= message_4_B2 <= 1 Integer
0 <= message_4_B3 <= 1 Integer
0 <= message_4_B4 <= 1 Integer
0 <= message_4_B5 <= 1 Integer
0 <= message_5_B1 <= 1 Integer
0 <= message_5_B2 <= 1 Integer
0 <= message_5_B3 <= 1 Integer
0 <= message_5_B4 <= 1 Integer
0 <= message_5_B5 <= 1 Integer
0 <= message_6_B1 <= 1 Integer
0 <= message_6_B2 <= 1 Integer
0 <= message_6_B3 <= 1 Integer
0 <= message_6_B4 <= 1 Integer
0 <= message_6_B5 <= 1 Integer
0 <= message_7_B1 <= 1 Integer
0 <= message_7_B2 <= 1 Integer
0 <= message_7_B3 <= 1 Integer
0 <= message_7_B4 <= 1 Integer
0 <= message_7_B5 <= 1 Integer

/usr/local/lib/python2.7/site-packages/pulp/solverdir/cbc/linux/64/cbc /tmp/1-pulp.mps branch printingOptions all solution /tmp/1-pulp.sol

# Result of Optimizing Partition Replica Move
Optimizer Status: Optimal
Number of Parition Replica Movements: 9.0

# CURRENT ASSIGNMENT
broker          1       2       3       4       5
message_0       1       1       1       0       0
message_1       1       1       1       0       0
message_2       1       1       1       0       0
message_3       1       1       1       0       0
message_4       1       1       1       0       0
message_5       1       1       1       0       0
message_6       1       1       1       0       0
message_7       1       1       1       0       0


# PROPOSED ASSIGNMENT
broker          1       2       3       4       5
message_0       0       1       1       0       1
message_1       1       1       1       0       0
message_2       0       0       1       1       1
message_3       1       1       1       0       0
message_4       1       1       0       1       0
message_5       1       0       1       0       1
message_6       1       0       0       1       1
message_7       0       1       0       1       1

{"vertion": 1, "partitions": [{"topic": "message", "partition": 0, "replicas": [2, 3, 5]}, {"topic": "message", "partition": 1, "replicas": [1, 2, 3]}, {"topic": "message", "partition": 2, "replicas": [3, 4, 5]}, {"topic": "message", "partition": 3, "replicas": [1, 2, 3]}, {"topic": "message", "partition": 4, "replicas": [1, 2, 4]}, {"topic": "message", "partition": 5, "replicas": [1, 3, 5]}, {"topic": "message", "partition": 6, "replicas": [1, 4, 5]}, {"topic": "message", "partition": 7, "replicas": [2, 4, 5]}]}
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
