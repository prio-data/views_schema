
from unittest import TestCase
from views_schema import TimeSpan, Partition, Partitions

class TestPartitioning(TestCase):
    def test_extent(self):
        p = Partition(timespans ={
            "a": TimeSpan(start=1, end = 5),
            "b": TimeSpan(start=6, end = 10)
            })
        start,end = p.extent()
        self.assertEqual(start,1)
        self.assertEqual(end,10)

        p = Partition(timespans ={
            "a": TimeSpan(start=1, end = 1),

            "b": TimeSpan(start=2, end = 72),
            "c": TimeSpan(start=4, end = 25),
            "d": TimeSpan(start=8, end = 10),
            "e": TimeSpan(start=10, end = 83),
            "f": TimeSpan(start=52, end = 60),

            "z": TimeSpan(start=10, end = 100)
            })
        start,end = p.extent()
        self.assertEqual(start, 1)
        self.assertEqual(end, 100)

    def test_partitions_extent(self):
        ps = Partitions(partitions = {
            "A": Partition(timespans = {
                "a": TimeSpan(start = 1,end=10),
                "b": TimeSpan(start=11, end = 20)
                }),
            "b": Partition(timespans = {
                "a": TimeSpan(start = 21,end=30),
                "b": TimeSpan(start=31, end = 40)
                })
            })

        start,end = ps.extent()
        self.assertEqual(start,1)
        self.assertEqual(end,40)

    def test_continuous(self):
        p = Partition(timespans = {
            "a": TimeSpan(start=1,end=10),
            "b": TimeSpan(start=11,end=20)
            })
        self.assertTrue(p.continuous)

        p = Partition(timespans = {
            "a": TimeSpan(start=1,end=5),
            "b": TimeSpan(start=11,end=20),
            "c": TimeSpan(start=4,end=7)
            })
        self.assertTrue(not p.continuous)

    def test_timespan_map(self):
        ts = TimeSpan(start = 1, end = 10)
        ts_a = ts.map(lambda s,e: (1,1))
        self.assertEqual(ts_a.start,1)
        self.assertEqual(ts_a.end,1)

        ts_b = ts.map((lambda s,e: map(lambda t: t if t < 5 else 5, (s,e))))
        self.assertEqual(ts_b.start, 1)
        self.assertEqual(ts_b.end, 5)

        ts_c = ts.map(lambda s,e: (s+1,e-1))
        self.assertEqual(ts_c.start,2)
        self.assertEqual(ts_c.end,9)

    def test_partition_map(self):
        p = Partition(timespans = {
            "a":TimeSpan(start=1, end=10),
            "b":TimeSpan(start=11, end=20)
            })

        p_a = p.map(lambda s,e: (1,1))
        ts_a_a,ts_a_b = p_a.timespans.values()
        self.assertEqual({ts_a_a.start, ts_a_a.end, ts_a_b.start, ts_a_b.end}, {1})

        p_b = p.map(lambda s,e: (s+1,e-1))
        ts_b_a,ts_b_b = p_b.timespans.values()
        self.assertEqual(ts_b_a.start,2)
        self.assertEqual(ts_b_a.end,9)
        self.assertEqual(ts_b_b.start,12)
        self.assertEqual(ts_b_b.end,19)

    def test_partitions_map(self):
        ps = Partitions(partitions = {
            "A": Partition(timespans = {
                "a": TimeSpan(start = 1, end = 10),
                "b": TimeSpan(start = 11, end = 20)
                })
            })

        s,e = ps.map(lambda s,e: (1,1)).extent()
        self.assertEqual({s,e},{1})

        s,e = ps.map(lambda s,e: (s+9, e-10)).extent()
        self.assertEqual({s,e},{10})

    def test_timespan_union(self):
        ts = TimeSpan(start=1,end=5).union(TimeSpan(start=4,end=7))
        self.assertEqual(ts.start, 4)
        self.assertEqual(ts.end, 5)
        self.assertIsNone(TimeSpan(start=1,end=3).union(TimeSpan(start=4,end=7)))

    def test_timespan_to_partition(self):
        p = TimeSpan(start = 1, end = 10).to_partition({"a":.5, "b":.5})
        tgt = Partition(timespans = {
            "a":TimeSpan(start = 1, end = 5),
            "b":TimeSpan(start = 6, end = 10)
            })
        self.assertEqual(p, tgt)
        p = TimeSpan(start = 1, end = 100).to_partition({"a":.25, "b":.25, "c":.5})
        tgt = Partition(timespans = {
            "a":TimeSpan(start = 1, end = 25),
            "b":TimeSpan(start = 26, end = 50),
            "c":TimeSpan(start = 51, end = 100)
            })
        self.assertEqual(p,tgt)

    def test_overlap_check(self):
        p = TimeSpan(start = 1, end = 100).to_partition({"a": .25, "b": .25, "c": .25, "d": .25})
        self.assertFalse(p.has_overlap)
        self.assertTrue(p.map(lambda s,e: (s,e+10)).has_overlap)

    def test_no_overlap_fix(self):
        p = (TimeSpan(start = 1, end = 100)
                .to_partition({"a": .25, "b": .25, "c": .25, "d": .25})
                .map(lambda s,e: (s,e+10))
                .no_overlap())
        self.assertFalse(p.has_overlap)
        self.assertEqual(p.timespans["a"].end, 35)
        self.assertEqual(p.timespans["b"].start, 36)

        ps = (Partitions(partitions = {"A": p})
            .map(lambda s,e: (s,e+10))
            .pmap(lambda p: p.no_overlap()))

        self.assertTrue(all(map(lambda p: not p.has_overlap, ps.partitions.values())))

