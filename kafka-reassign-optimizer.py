#! /usr/bin/env python
"""
kafka-reassign-optimizer.py
  This scripts generates optimal replica assignment in terms of the number of replica move.
  usage:
    kafka-reassign-optimizer.py < current_partition_replica_assignment.json

    current_partition_replica_assignment.json is like below:
    {
      "brokers": "1,2,3,4,5,6",
      "balance_parameters": {
        "min_factor": 0.5,
        "max_factor": 1.5
      }
      "partitions": {
        "version": 1,
        "partitions": [
            {"topic": "t1", "partition": 0, "replicas": [1,3,5]},
            ...
        ]
      }
      // pinned_replicas are optional
      "pinned_replicas": [
        {"topic": "t1", "partition": 0, "replica": 3 },
        ...
      ]
    }
"""
import os
import sys
import logging
import math
from itertools import groupby, product
import json
import pulp
import random

BALANCE_MIN_FACTOR = 0.9
BALANCE_MAX_FACTOR = 1.1

def propose_assignment_with_minimum_move(config):
    logger.info("# Configurations for reassignment partition replicas")
    logger.info("brokers= %s", config.brokers)
    if config.new_replication_factor > 0:
        logger.info("new_replication_factor= %s", config.new_replication_factor)
    logger.info("topics= %s", config.topics)
    logger.info("partitions=%s", [ (t, len(config.topic2partition[t])) for t in config.topic2partition ])
    logger.info("total_partitions= %s", config.total_partitions)
    logger.info("total_replicas_to_assign= %s", config.total_replicas)
    logger.info("total_replica_weight= %s", config.total_replica_weight)
    if config.pinned_replicas:
        logger.info("pinned parition replica: %s", config.pinned_replicas)
    if config.balanced_load_min == config.balanced_load_max:
        logger.info("balanced_load= %s", config.balanced_load_min)
    else:
        logger.info("balanced_load_min= %s", config.balanced_load_min)
        logger.info("balanced_load_max= %s", config.balanced_load_max)

    # binary integer programming instance
    problem = pulp.LpProblem('Minimize Parition Replica Movement for Partition Reassignment', pulp.LpMinimize)

    # varaiable
    proposed_assignment = dict()
    for (t,p,b) in config.tpbs:
        if config.pinned_replicas.has_key((t,p,b)):
            proposed_assignment[(t,p,b)] = pulp.LpVariable("{}_{}_B{}".format(t,p,b), 1, 1, 'Integer')
        else:
            proposed_assignment[(t,p,b)] = pulp.LpVariable("{}_{}_B{}".format(t,p,b), 0, 1, 'Integer')

    # objective function: number of newly assigned partitions
    num_movement = 0
    for (t,p,b) in config.tpbs:
        if config.current_assignment[(t,p,b)] == 0:
            num_movement += proposed_assignment[(t,p,b)] * config.partition_weights[(t,p)]
        else:
            num_movement += ((proposed_assignment[(t,p,b)] - 1) * -1 * config.partition_weights[(t, p)])
    problem += (num_movement / 2.0)

    #
    # constraints
    #
    # c1: total_replicas
    _total_replica_weight = 0
    for (t, p, b) in config.tpbs:
        _total_replica_weight += config.partition_weights[(t,p)] * proposed_assignment[(t,p,b)]
    problem += _total_replica_weight == config.total_replica_weight, "total replicas weight is %s" % config.total_replica_weight

    # c2: each partition has replication factor
    for (t, p) in config.tps:
        replicas = 0
        current_replicas = 0
        for b in config.brokers:
            replicas += proposed_assignment[(t,p,b)]
            current_replicas += config.current_assignment[(t,p,b)]
        if config.new_replication_factor > 0:
            problem += replicas == config.new_replication_factor, "partition (%s,%s) has replication factor %s" % (t,p, config.new_replication_factor)
        else:
            problem += replicas == current_replicas, "partition (%s,%s) has replication factor %s" % (t,p, current_replicas)

    # c3: new assignment is well_balanced
    for b in config.brokers:
        score = 0
        for (t, p) in config.tps:
            score += config.partition_weights[(t,p)] * proposed_assignment[(t, p, b)]
        if config.balanced_load_max == config.balanced_load_min:
            problem += score == config.balanced_load_max, "load of broker %s is balanced." % b
        else:
            problem += score >= config.balanced_load_min, "load of broker %s is greater than or equal to %s" % (b, config.balanced_load_min)
            problem += score <= config.balanced_load_max, "load of broker %s is less than or equal to %s" % (b, config.balanced_load_max)

    logger.debug("\n# Binary Integer Programming")
    logger.debug(problem)
    status = problem.solve()

    logger.info("\n# Result of Optimizing Partition Replica Move")
    if status == pulp.LpStatusOptimal:
        logger.info("Optimizer Status: %s", pulp.LpStatus[status])
    else:
        logger.error("Optimizer Status: %s", pulp.LpStatus[status])
    logger.info("Number of Parition Replica Movements: %s", pulp.value(problem.objective))

    logger.debug("\n# CURRENT ASSIGNMENT")
    logger.debug(
        assignment_str(
            config,
            config.current_assignment)
    )
    logger.debug("\n# PROPOSED ASSIGNMENT")
    logger.debug(
        assignment_str(
            config,
            dict((k, proposed_assignment[k].value()) for k in proposed_assignment))
    )

    return (problem, status, proposed_assignment)

def assignment_str(config, _assignment):
    str = []
    str.append("broker \t\t")
    for b in config.brokers:
        str.append("%s\t" % b)
    str.append("\n")
    for (t,p) in config.tps:
        str.append("%s_%s\t" % (t, p))
        for b in config.brokers:
            v = config.partition_weights[(t,p)] if _assignment[(t,p,b)] == 1 else 0
            str.append("%s\t" % v)
        str.append("\n")
    return ''.join(str)

class ReassignmentOptimizerConfig:
    def __init__(self, _json):
        __logger = logging.getLogger(__name__)
        self.__json = _json
        self.brokers = sorted([ int(b) for b in _json["brokers"].split(',') ])
        self.new_replication_factor = int(_json["new_replication_factor"]) if _json.has_key("new_replication_factor") else -1

        _t2p = dict()
        _tp = sorted([(p["topic"], p["partition"]) for p in _json["partitions"]["partitions"]], key=lambda x: (x[0],x[1]))
        for k, g in groupby(_tp, lambda x: x[0]):
            _t2p[k] = map(lambda x: x[1],list(g))
        self.topic2partition = _t2p
        self.topics = list(_t2p.keys())

        _tps = []
        _tpbs = []
        for t in sorted(list(_t2p.keys())):
            for p in sorted(_t2p[t]):
                _tps.append((t,p))
                for b in sorted(self.brokers):
                    _tpbs.append((t,p,b))

        # list of tuples: it's only for avoid nested loops
        self.tpbs = _tpbs
        self.tps = _tps

        _assignment = dict(sum([map(lambda r: ((p["topic"], p["partition"], r), 1 ), p["replicas"]) for p in _json["partitions"]["partitions"]],[]))
        for (t,p,b) in self.tpbs:
            if not _assignment.has_key((t, p, b)):
                _assignment[(t,p,b)] = 0
        self.current_assignment = _assignment

        self.total_partitions = sum([ len(_t2p[t]) for t in _t2p], 0)
        if self.new_replication_factor > 0:
            self.total_replicas = total_partitions * new_replication_factor
        else:
            self.total_replicas = sum([ len(p["replicas"]) for p in _json["partitions"]["partitions"] ], 0)

        self.pinned_replicas = None
        __pinned_replicas = dict()
        if self.__json.has_key("pinned_replicas"):
            for j in self.__json["pinned_replicas"]:
                t = j["topic"]
                p = j["partition"]
                r = j["replica"]
                if not __pinned_replicas.has_key((t,p,r)):
                    if (t,p,r) in self.tpbs:
                        __pinned_replicas[(t,p,r)] = 1 # the value is meaningless.
                    else:
                        __logger.warning("broker %s will be vanished in proposed assinment!! replica %s of partition (%s, %s) won't be pinned in proposed assignment!!" % (r, t, p))
        self.pinned_replicas = __pinned_replicas

        self.partition_weights = None
        __partition_weights = dict()
        if self.__json.has_key("partition_weights"):
            for j in self.__json["partition_weights"]:
                t = j["topic"]
                p = j["partition"]
                w = j["weight"]
                if not __partition_weights.has_key((t,p)):
                    if (t,p) in self.tps:
                        __partition_weights[(t,p)] = w
        else:
            # defualt movement weight is all one.
            for (t,p) in self.tps:
                __partition_weights[(t,p)] = 1.0
        self.partition_weights = __partition_weights

        if self.__json.has_key("balance_parameters"):
            if self.__json["balance_parameters"].has_key("min_factor"):
                self.balance_min_factor = self.__json["balance_parameters"]["min_factor"]
            else:
                self.balance_max_factor = BALANCE_MAX_FACTOR
            if self.__json["balance_parameters"].has_key("max_factor"):
                self.balance_max_factor = self.__json["balance_parameters"]["max_factor"]
            else:
                self.balance_max_factor = BALANCE_MAX_FACTOR
        else:
            self.balance_max_factor = BALANCE_MAX_FACTOR
            self.balance_min_factor = BALANCE_MIN_FACTOR

        self.total_replica_weight = 0
        for (t,p,b) in self.tpbs:
            if self.current_assignment[(t,p,b)] == 1:
                self.total_replica_weight += __partition_weights[(t,p)]

        self.balanced_load_min = int(math.floor(self.total_replica_weight / len(self.brokers) * self.balance_min_factor))
        self.balanced_load_max = int(math.ceil(self.total_replica_weight / len(self.brokers) * self.balance_max_factor))

    #     _pweight = dict()
    #     for (t,p) in self.tps:
    #         _pweight[(t,p)] = 1
    #     self.partition_weight = _pweight
    #
    # def set_partition_weights(tp2w):
    #     """ tp2w: dict (t,p) -> weight"""
    #     self.partition_weight = tp2w

if __name__ == '__main__':
    logger = logging.getLogger()
    if os.environ.has_key("LOGLEVEL"):
        level = getattr(logging, os.environ["LOGLEVEL"].upper())
    else:
        level = logging.INFO
    logger.setLevel(level)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(level)
    logger.addHandler(ch)

    loaded_json = json.load(sys.stdin, encoding='utf-8')
    reassignment_config = ReassignmentOptimizerConfig(loaded_json)
    (problem, status, proposed_assignment) = propose_assignment_with_minimum_move(reassignment_config)

    logger.info("")
    logger.info("# Partition Replica Assignment (copy it and input to kafka-reassign-partition.sh)")
    proposed_assignment_json = { 'vertion': 1, 'partitions':[] }
    for (t, p) in reassignment_config.tps:
        replicas = []
        for b in reassignment_config.brokers:
            if int(proposed_assignment[(t,p,b)].value()) == 1:
                replicas.append(b)
        # currently proposed_assignment returns sorted replica list and this is quick hack to distribute leader node and preffered node.
        # TODO: select leaders according to the current status and the cluster balance.
        random.shuffle(replicas)
        proposed_assignment_json["partitions"].append({
            "topic": t,
            "partition": int(p),
            "replicas": replicas
        })
    print json.dumps(proposed_assignment_json)
