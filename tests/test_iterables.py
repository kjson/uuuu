"""Tests for iterables."""
import unittest
import time
from uuuu import iterables

# Move the fake function outside of the class to make it picklable by multiprocessing
def fake_func(num):
    """Fake function to use in parallelize tests."""
    return num + 1


class TestIterables(unittest.TestCase):
    """Tests for functions in the iterables module."""

    def setUp(self):
        """Common setup for the test cases."""
        self.items_range_10 = range(10)
        self.items_range_1000 = range(1000)
        self.single_item = [1]

    def test_batches(self):
        """Test the batches function batches an iterable into tuples."""
        self.assertEqual(
            list(iterables.batches(self.items_range_10, 3)),
            [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]
        )
        self.assertEqual(list(iterables.batches([], 2)), [])
        self.assertEqual(
            list(iterables.batches(
                self.items_range_10, 20)), [tuple(self.items_range_10)])

        # Test invalid batch size
        with self.assertRaises(ValueError):
            list(iterables.batches(self.items_range_10, 1))

    def test_peek(self):
        """Test the peek function on an iterable and iterator."""
        peek, original = iterables.peek(self.items_range_10, 2)
        self.assertEqual(list(peek), [0, 1])
        self.assertEqual(list(original), list(self.items_range_10))

        peek, original = iterables.peek(iter(self.items_range_10), 2)
        self.assertEqual(list(peek), [0, 1])
        self.assertEqual(list(original), list(self.items_range_10))

        # Test with empty iterator
        peek, original = iterables.peek(iter([]), 2)
        self.assertEqual(list(peek), [])
        self.assertEqual(list(original), [])

    def test_multimap(self):
        """Test that multimap applies multiple functions to an iterable."""
        functions = (lambda x: x + 2, lambda x: x - 1, lambda x: x - 1)
        self.assertEqual(list(iterables.multimap(
            functions, self.items_range_10)), list(self.items_range_10))
        self.assertEqual(list(iterables.multimap(
            functions, iter(self.items_range_10))), list(self.items_range_10))
        self.assertEqual(list(iterables.multimap(
            [], self.items_range_10)), list(self.items_range_10))

    def test_split(self):
        """Test that split divides an iterable based on a predicate."""
        true, false = iterables.split(lambda x: x % 2 == 0, self.items_range_10)
        self.assertEqual(list(true), [0, 2, 4, 6, 8])
        self.assertEqual(list(false), [1, 3, 5, 7, 9])

        true, false = iterables.split(lambda x: x % 2 == 0, iter(self.items_range_10))
        self.assertEqual(list(true), [0, 2, 4, 6, 8])
        self.assertEqual(list(false), [1, 3, 5, 7, 9])

        # Test split on empty iterator
        true, false = iterables.split(lambda x: x % 2 == 0, iter([]))
        self.assertEqual(list(true), [])
        self.assertEqual(list(false), [])

    def test_exhaust(self):
        """Test that exhaust consumes an iterable fully."""
        items = iter(self.items_range_10)
        iterables.exhaust(items)
        self.assertEqual(list(items), [])

        items = self.items_range_10
        iterables.exhaust(items)
        self.assertEqual(list(items), list(self.items_range_10))

    def test_parallelize(self):
        """Test that parallelize applies a function in parallel."""
        result = sorted(iterables.parallelize(fake_func, self.items_range_10))
        expected = sorted([x + 1 for x in self.items_range_10])
        self.assertEqual(result, expected)

        # Test parallelize on empty iterator
        result = list(iterables.parallelize(fake_func, iter([])))
        self.assertEqual(result, [])

    def test_roundrobin(self):
        """Test that roundrobin yields items in the correct round-robin order."""
        result = list(iterables.roundrobin([1, 2, 3], ['a', 'b', 'c']))
        self.assertEqual(result, [1, 'a', 2, 'b', 3, 'c'])

        result = list(iterables.roundrobin([1, 2], ['a', 'b', 'c'], [100]))
        self.assertEqual(result, [1, 'a', 100, 2, 'b', 'c'])

        result = list(iterables.roundrobin([1, 2, 3]))
        self.assertEqual(result, [1, 2, 3])

        result = list(iterables.roundrobin())
        self.assertEqual(result, [])

        result = list(iterables.roundrobin([], [], []))
        self.assertEqual(result, [])

        result = list(iterables.roundrobin([], [1, 2, 3]))
        self.assertEqual(result, [1, 2, 3])

        result = list(iterables.roundrobin([1, 2, 3], []))
        self.assertEqual(result, [1, 2, 3])

        # Additional test with different iterable lengths
        result = list(iterables.roundrobin([1, 2], [3, 4, 5, 6], [7]))
        self.assertEqual(result, [1, 3, 7, 2, 4, 5, 6])

    def test_drop_random(self):
        """Test drop_random samples items from an iterable based on a probability rate."""
        result = list(iterables.drop_random(self.items_range_10, 1.0))
        self.assertEqual(result, list(self.items_range_10))

        result = list(iterables.drop_random(self.items_range_10, 0.0))
        self.assertEqual(result, [])

        result = list(iterables.drop_random(self.items_range_1000, 0.5))
        self.assertTrue(400 <= len(result) <= 600)

        with self.assertRaises(ValueError):
            list(iterables.drop_random(self.items_range_10, -0.1))

        with self.assertRaises(ValueError):
            list(iterables.drop_random(self.items_range_10, 1.1))

        result = list(iterables.drop_random([], 0.5))
        self.assertEqual(result, [])

        result = list(iterables.drop_random(self.single_item, 1.0))
        self.assertEqual(result, self.single_item)

        result = list(iterables.drop_random(self.single_item, 0.0))
        self.assertEqual(result, [])

        items = range(1_000_000)
        result = list(iterables.drop_random(items, 0.0001))
        self.assertTrue(len(result) > 0)

        # Test with very high rate (0.99) and very low rate (0.01)
        result_high = list(iterables.drop_random(self.items_range_1000, 0.99))
        result_low = list(iterables.drop_random(self.items_range_1000, 0.01))
        self.assertTrue(len(result_high) > len(result_low))

    def test_time_limited_stream(self):
        """Test time_limited_stream stops yielding items after the time limit."""
        # Use a slightly longer time limit with delay between items
        result = list(iterables.time_limited_stream(
            self.items_range_1000, 0.1, delay_per_item=0.005))

        # Expect at least 1 item, but fewer than 20
        self.assertTrue(1 <= len(result) < 20)

    def test_filter_with_state(self):
        """Test filter_with_state yields items based on a stateful predicate."""
        # Only yield if the current item is greater than the previous
        result = list(iterables.filter_with_state(
            [1, 2, 1, 3, 2, 4], lambda prev, curr: curr > prev))
        self.assertEqual(result, [1, 2, 3, 4])

    def test_rolling_aggregate(self):
        """Test rolling_aggregate applies a rolling aggregation to the items."""
        # Simple rolling sum
        result = list(iterables.rolling_aggregate(
            self.items_range_10, lambda x, y: x + y))
        self.assertEqual(result, [0, 1, 3, 6, 10, 15, 21, 28, 36, 45])

    def test_throttle(self):
        """Test throttle limits the rate of items yielded."""
        # Use a higher rate of 1000 items per second for testing to reduce execution time
        start_time = time.time()
        result = list(iterables.throttle(self.items_range_10, 1000))
        end_time = time.time()

        # Allow a slightly higher execution time threshold to account
        # for system overhead
        # Ensure it finishes quickly but allow some overhead
        self.assertTrue(end_time - start_time < 0.05)
        self.assertEqual(result, list(self.items_range_10))

    def test_inner_join(self):
        """Test inner_join joins two streams based on a key."""
        stream1 = [{'id': 1, 'value': 'a'}, {'id': 2, 'value': 'b'}, {'id': 3, 'value': 'c'}]
        stream2 = [{'id': 1, 'other_value': 'x'}, {'id': 3, 'other_value': 'y'}]

        result = list(iterables.inner_join(
            stream1, stream2, key1=lambda x: x['id'], key2=lambda x: x['id']))
        self.assertEqual(result, [
            ({'id': 1, 'value': 'a'}, {'id': 1, 'other_value': 'x'}),
            ({'id': 3, 'value': 'c'}, {'id': 3, 'other_value': 'y'}),
        ])


if __name__ == '__main__':
    unittest.main()
