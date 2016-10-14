#! /usr/bin/env python
"""
kafka-reassign-optimizer.py
  This scripts generates optimal replica assignment in terms of the number of replica move.
  usage:
    kafka-reassign-optimizer.py < current_partition_replica_assignment.json

    current_partition_replica_assignment.json is like below:
    {
      "brokers": "1,2,3,4,5,6",
      "partitions": {
        "version": 1,
        "partitions": [
            {"topic": "t1", "partition": 0, "replicas": [1,3,5]},
            ...
        ]
      }
    }
"""
import sys
from logging import getLogger, StreamHandler, DEBUG
import math
from itertools import groupby, product
import json
import pulp

def propose_assignment_with_minimum_move(spec):
    logger.info("# input information for making assignment proposal")
    logger.info("brokers= %s", spec.brokers)
    logger.info("new_replication_factor= %s", spec.new_replication_factor)
    logger.info("topics= %s", spec.topics)
    logger.info("partitions=%s", [ (t, len(spec.topic2partition[t])) for t in spec.topic2partition ])
    logger.info("total_partitions= %s", spec.total_partitions)
    logger.info("total_replicas_to_assign= %s", spec.total_replicas)
    if spec.balanced_load_min == spec.balanced_load_max:
        logger.info("balanced_load= %s", spec.balanced_load_min)
    else:
        logger.info("balanced_load_min= %s", spec.balanced_load_min)
        logger.info("balanced_load_max= %s", spec.balanced_load_max)

    # binary integer programming instance
    problem = pulp.LpProblem('Minimize Parition Replica Movement for Partition Reassignment', pulp.LpMinimize)

    # varaiables
    proposed_assignment = dict()
    for (t,p,b) in spec.tpbs:
        proposed_assignment[(t,p,b)] = pulp.LpVariable("{}_{}_B{}".format(t,p,b), 0, 1, 'Integer')

    # objective function: number of newly assigned partitions
    num_movement = 0
    for (t,p,b) in spec.tpbs:
        if spec.current_assignment[(t,p,b)] == 0:
            # num_movement += spec.partition_weight[(t,p)] * proposed_assignment[(t,p,b)]
            num_movement += proposed_assignment[(t,p,b)]
    problem += num_movement

    #
    # constraints
    #
    # c1: total_replicas
    _total_replicas = 0
    for (t, p, b) in spec.tpbs:
        _total_replicas += proposed_assignment[(t,p,b)]
    problem += _total_replicas == spec.total_replicas, "number or total replicas is %s" % spec.total_replicas

    # c2: each partition has replication factor
    for (t, p) in spec.tps:
        replicas = 0
        current_replicas = 0
        for b in spec.brokers:
            replicas += proposed_assignment[(t,p,b)]
            current_replicas += spec.current_assignment[(t,p,b)]
        if spec.new_replication_factor > 0:
            problem += replicas == spec.new_replication_factor, "partition (%s,%s) has replication factor %s" % (t,p, spec.new_replication_factor)
        else:
            problem += replicas == current_replicas, "partition (%s,%s) has replication factor %s" % (t,p, current_replicas)

    # c3: new assignment is well_balanced
    for b in spec.brokers:
        score = 0
        for (t, p) in spec.tps:
            score += proposed_assignment[(t, p, b)]
        if spec.balanced_load_max == spec.balanced_load_min:
            problem += score == spec.balanced_load_max, "load of broker %s is balanced." % b
        else:
            problem += score >= spec.balanced_load_min, "load of broker %s is greater than or equal to %s" % (b, spec.balanced_load_min)
            problem += score <= spec.balanced_load_max, "load of broker %s is less than or equal to %s" % (b, spec.balanced_load_max)

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
            spec,
            spec.current_assignment)
    )
    logger.debug("\n# PROPOSED ASSIGNMENT")
    logger.debug(
        assignment_str(
            spec,
            dict((k, proposed_assignment[k].value()) for k in proposed_assignment))
    )

    return (problem, status, proposed_assignment)

def assignment_str(spec, _assignment):
    str = []
    str.append("broker \t\t")
    for b in spec.brokers:
        str.append("%s\t" % b)
    str.append("\n")
    for (t,p) in spec.tps:
        str.append("%s_%s\t" % (t, p))
        for b in spec.brokers:
            str.append("%s\t" % int(_assignment[(t,p,b)]))
        str.append("\n")
    return ''.join(str)

class ReassignmentSpec:
    def __init__(self, _json):
        self._underlying_json = _json
        self.brokers = sorted([ int(b) for b in _json["brokers"].split(',') ])
        self.new_replication_factor = int(loaded_json["new_replication_factor"]) if loaded_json.has_key("new_replication_factor") else -1

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
        self.balanced_load_min = int(math.floor(1.0*self.total_replicas / len(self.brokers)))
        self.balanced_load_max = int(math.ceil(1.0*self.total_replicas / len(self.brokers)))

    #     _pweight = dict()
    #     for (t,p) in self.tps:
    #         _pweight[(t,p)] = 1
    #     self.partition_weight = _pweight
    #
    # def set_partition_weights(tp2w):
    #     """ tp2w: dict (t,p) -> weight"""
    #     self.partition_weight = tp2w

if __name__ == '__main__':
    logger = getLogger()
    logger.setLevel(DEBUG)
    ch = StreamHandler(sys.stderr)
    ch.setLevel(DEBUG)
    logger.addHandler(ch)

    loaded_json = json.load(sys.stdin, encoding='utf-8')
    reassignment_spec = ReassignmentSpec(loaded_json)
    (problem, status, proposed_assignment) = propose_assignment_with_minimum_move(reassignment_spec)

    proposed_assignment_json = { 'vertion': 1, 'partitions':[] }
    for (t, p) in reassignment_spec.tps:
        replicas = []
        for b in reassignment_spec.brokers:
            if int(proposed_assignment[(t,p,b)].value()) == 1:
                replicas.append(b)
        proposed_assignment_json["partitions"].append({
            "topic": t,
            "partition": int(p),
            "replicas": replicas
        })
    print json.dumps(proposed_assignment_json)
