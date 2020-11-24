"""Tests some of the stuff in the iterables module."""
import unittest

from uuuu import iterables

class TestIterables(unittest.TestCase):
    """Tests for functions in the iterables module."""

    def test_batch(self):
        """Test that we can batch an iterable into tuples."""
        self.assertEqual(list(iterables.batches(range(10), 3)), [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)])
        self.assertEqual(list(iterables.batches([], 2)), [])
        self.assertEqual(list(iterables.batches(range(10), 20)), [tuple(range(10))])

    def test_peek(self):
        """Test that do a benign peek of an interable."""

        # Test that it works on a normal iterable.
        peek, original = iterables.peek(range(10), 2)
        self.assertEqual(list(peek), ([0, 1]))
        self.assertEqual(list(original), list(range(10)))

        # Test that it works on an iterator.
        peek, original = iterables.peek(iter(range(10)), 2)
        self.assertEqual(list(original), list(range(10)))
        self.assertEqual(list(peek), ([0, 1]))

    def test_multimap(self):
        """Test that we get the expected results from multimap"""
        functions = (lambda x: x + 2, lambda x: x - 1, lambda x: x - 1)
        # Test that a chain of functions works the way we'd expect.
        self.assertEqual(list(iterables.multimap(functions, range(10))), list(range(10)))
        # Make sure it works on iterators as well.
        self.assertEqual(list(iterables.multimap(functions, iter(range(10)))), list(range(10)))
        # Make sure that passing in no functions is a no-op.
        self.assertEqual(list(iterables.multimap([], range(10))), list(range(10)))

    def test_split(self):
        """Test that we can create two iterables based on a truthy predicate."""
        true, false = iterables.split(lambda x: x % 2 == 0, range(10))
        self.assertEqual(list(true), [0, 2, 4, 6, 8])
        self.assertEqual(list(false), [1, 3, 5, 7, 9])

        # Make sure it still works for iterators.
        true, false = iterables.split(lambda x: x % 2 == 0, iter(range(10)))
        self.assertEqual(list(true), [0, 2, 4, 6, 8])
        self.assertEqual(list(false), [1, 3, 5, 7, 9])

    def test_exhaust(self):
        """Simple tests for exhaust."""
        items = iter(range(10))
        iterables.exhaust(items)
        self.assertEqual(list(items), [])

        # Just to illustrate how it works on iterables.
        items = range(10)
        iterables.exhaust(items)
        self.assertEqual(list(items), list(range(10)))
