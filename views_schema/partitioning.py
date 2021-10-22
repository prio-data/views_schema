"""
These models are related to time-partitioning. In ViEWS,
time-partitions are defined as nested dictionaries.
"""
from operator import or_
from functools import reduce, partial
from collections import defaultdict
from typing import Optional, Dict, Tuple, Callable, List
import pydantic

smallest, biggest = (partial(reduce,fn) for fn in (min,max))

# =PARTITIONING MODELS====================================

class TimeSpan(pydantic.BaseModel):
    """
    A span of time, between start and end
    """
    start: int
    end: int

    def map(self, fn: Callable[[int,int], Tuple[int,int]])-> 'TimeSpan':
        """
        calls fn on start and end, yielding a new timespan with fn(start,end)
        """
        start,end = fn(self.start, self.end)
        return TimeSpan(start = start, end = end)

    def __iter__(self):
        for t in [self.start, self.end]:
            yield t

    def __eq__(self, other: 'TimeSpan')-> bool:
        return self.start == other.start and self.end == other.end

    def is_within(self, other: 'TimeSpan') -> bool:
        """
        Does timespan cover other timespan?
        """
        return self.start >= other.start and self.end <= other.end

    def overlaps(self, other: 'TimeSpan') -> bool:
        """
        Does timespan overlap with other timespan?
        """
        return self.union(other) is not None

    def union(self,other: 'TimeSpan')-> Optional['TimeSpan']:
        """
        Returns a timespan which is the union of self and other.
        """
        start = biggest((self.start, other.start))
        end = smallest((self.end, other.end))
        if start <= end:
            return TimeSpan(start = start, end = end)
        else:
            return None

    def times(self):
        """
        Yields each time for t between start and end
        """
        s,e = self
        for t in range(s,e+1):
            yield t

    def to_partition(self, percentages: Dict[str, float]) -> 'Partition':
        """
        Divide a timespan into multiple timespans, creating a Partition.
        """
        try:
            assert sum(percentages.values()) == 1
        except AssertionError:
            raise ValueError("Percentages must add up to 1")

        times = [*self.times()]
        n_times = len(times)
        timespans = {}
        current_pst = 0.0
        for key, value in percentages.items():
            start_index = int(n_times * current_pst)
            current_pst += value
            end_index = int(n_times * current_pst) -1
            timespans[key] = TimeSpan(start = times[start_index], end = times[end_index])
        return Partition(timespans = timespans)

class Partition(pydantic.BaseModel):
    """
    A partition, defined as a dictionary of timespans
    """
    timespans: Dict[str, TimeSpan]

    def __eq__(self, other: 'Partition') -> bool:
        same = False

        matching_names = set(self.timespans.keys()) == set(other.timespans.keys())
        same |= matching_names
        if matching_names:
            for timespan_name in self.timespans.keys():
                same &= self.timespans[timespan_name] == other.timespans[timespan_name]

        return same

    def map(self, fn: Callable[[int,int],Tuple[int,int]]) -> 'Partition':
        """
        Maps fn onto each timespan, returning a new Partition object.
        """
        return Partition(timespans = {k:ts.map(fn) for k,ts in self.timespans.items()})

    def extent(self) -> TimeSpan:
        """
        Returns a TimeSpan corresponding to the extent of the Partition.
        """
        return TimeSpan(
                start = smallest([ts.start for ts in self.timespans.values()]),
                end = biggest([ts.end for ts in self.timespans.values()]))

    def _cont_times(self) -> List[int]:
        return [*self.extent().times()]

    def _times(self) -> List[int]:
        all_times = reduce(or_, [{*ts.times()} for ts in self.timespans.values()])
        return sorted(list(all_times))

    @property
    def continuous(self):
        """
        Do the timespans in partition constitute a continuous timespan?
        """
        return self._cont_times() == self._times()

class Partitions(pydantic.BaseModel):
    name: Optional[str]
    partitions: Dict[str, Partition]

    def map(self, fn: Callable[[int,int],Tuple[int,int]]) -> 'Partitions':
        return Partitions(
                name = self.name,
                partitions = {k:p.map(fn) for k,p in self.partitions.items()})

    def extent(self) -> TimeSpan:
        extents = [p.extent() for p in self.partitions.values()]
        return TimeSpan(
                start = smallest([e.start for e in extents]),
                end = biggest([e.end for e in extents]))

    @property
    def anonymous(self) -> bool:
        return self.name is None

    @classmethod
    def from_dict(cls, partitions: Dict[str, Dict[str, Tuple[int,int]]]) -> 'Partitions':
        nested_def_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        to_instantiate = {"partitions": nested_def_dict}

        for partition_name, partition in partitions.items():
            for timespan_name, (start,end) in partition.items():
                to_instantiate["partitions"][partition_name]["timespans"][timespan_name] = {
                        "start": start,
                        "end": end
                        }

        return cls(**to_instantiate)
