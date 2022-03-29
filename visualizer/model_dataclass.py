from typing import List, Set, Tuple, Dict
from dataclasses import dataclass, field

import marshmallow_dataclass
import marshmallow.validate
import json

DATA_ROOT = "/Users/saadmusani/Documents/GitHub/allocator1/json" # Full file path would be <DATA_ROOT>/testName/2022-03-15_19.32.28/0.json

# https://pynative.com/make-python-class-json-serializable/
# https://github.com/lovasoa/marshmallow_dataclass (pip3 install marshmallow-dataclass)
@dataclass
class Node:
    Id: int
    Tags: Set[str] = field(metadata={'allow_none': True})
    Resources: Dict[str, int]

@dataclass
class Shard:
    Id: int
    Tags: Set[str] = field(metadata={'allow_none': True})
    Demands: Dict[str, int]

@dataclass
class ClusterState:
    Nodes: Dict[int, Node]
    Shards: Dict[int, Shard]
    CurrentAssignment: Dict[int, List[int]] # shard ID -> list of node IDs # Will specify the current allocation

@dataclass
class Configuration:
    WithCapacity: bool
    WithLoadBalancing: bool
    WithTagAffinity: bool
    WithMinimalChurn: bool
    MaxChurn: int
    SearchTimeout: int
    VerboseLogging: bool
    Rf: int

@dataclass
class SimulationStep:
    ClusterState: ClusterState
    Configuration: Configuration
    TimeInMs: int
    T: int

@dataclass
class SimulationReader:
    path: str
    
    def __init__(self, testName: str, folder: str) -> None:
        self.path = DATA_ROOT + "/" + testName + "/" + folder

    def getSimulationStep(self, t: int) -> SimulationStep:
        with open(self.path + "/" + str(t) + ".json", 'r') as myfile:
            data=myfile.read()

        dataDict = json.loads(data)

        step_schema = marshmallow_dataclass.class_schema(SimulationStep)()

        return step_schema.load(dataDict)
