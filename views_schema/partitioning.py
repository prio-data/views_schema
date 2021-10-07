"""
These models are related to time-partitioning. In ViEWS,
time-partitions are defined as nested dictionaries.
"""
from typing import Dict,TypeVar, Tuple

T = TypeVar("T")

# =PARTITIONING MODELS====================================

PartitionDictionaries = Dict[str, Dict[str, T]]
PartitionTimes = PartitionDictionaries[Tuple[int,int]]
