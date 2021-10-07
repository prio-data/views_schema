"""
These models are related to time-partitioning. In ViEWS,
time-partitions are defined as nested dictionaries.
"""
from collections import defaultdict
from typing import Optional, Dict, Tuple
import pydantic

# =PARTITIONING MODELS====================================

class TimeSpan(pydantic.BaseModel):
    start: int
    end: int

class Partition(pydantic.BaseModel):
    timespans: Dict[str, TimeSpan]

class Partitions(pydantic.BaseModel):
    name: Optional[str]
    partitions: Dict[str, Partition]

    @property
    def anonymous(self):
        return self.name is None

    @classmethod
    def from_dict(cls, partitions: Dict[str, Dict[str, Tuple[int,int]]]):
        nested_def_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        to_instantiate = {"partitions": nested_def_dict}

        for partition_name, partition in partitions.items():
            for timespan_name, (start,end) in partition.items():
                to_instantiate["partitions"][partition_name]["timespans"][timespan_name] = {
                        "start": start,
                        "end": end
                        }

        return cls(**to_instantiate)
