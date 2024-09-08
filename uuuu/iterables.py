"""Basic functions on iterables returning iterators"""
import collections
import functools
import itertools
import multiprocessing
import random
import time

from typing import Iterable, Callable, Iterator, Tuple, List


def batches(items: Iterable, batch_size: int) -> Iterator[Tuple]:
    """Yield items in tuples of the given batch size."""
    if batch_size < 2:
        raise ValueError("Batch size must be greater than or equal to two.")

    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield tuple(batch)
            batch = []

    if batch:
        yield tuple(batch)


def drop_random(items: Iterable, rate: float) -> Iterator:
    """Yields from items with some probability"""
    if not 0 <= rate <= 1:
        raise ValueError("Rate must be between 0 and 1.")
    yield from (item for item in items if random.random() < rate)


def roundrobin(*iterables: Iterable) -> Iterator:
    """Yield items from multiple iterables in round-robin fashion."""
    iterators = [iter(it) for it in iterables]
    while iterators:
        for it in list(iterators):
            try:
                yield next(it)
            except StopIteration:
                iterators.remove(it)


def peek(items: Iterable, num_items: int) -> Tuple[Iterator, Iterator]:
    """
    Peek at the first `num_items` elements of the iterable.

    Returns a tuple of (peeked elements, original iterable). If the input is an
    iterator, chain the original items back to maintain full access.
    """
    if num_items < 0:
        raise ValueError("Cannot peek negative items from an iterable.")

    seq1, seq2 = itertools.tee(items, 2)
    peeked = itertools.islice(seq1, num_items)

    # Chain the original items back if the input is an iterator
    if isinstance(items, Iterator):
        return peeked, itertools.chain(seq2, items)

    return peeked, seq2


def multimap(functions: Iterable[Callable], items: Iterable) -> Iterator:
    """Apply multiple functions sequentially over the input iterable."""
    return functools.reduce(lambda i, f: map(f, i), functions, items)


def split(predicate: Callable, items: Iterable) -> Tuple[Iterator, Iterator]:
    """
    Split items based on a predicate into two iterables: one where the predicate is True,
    and one where it is False.
    """
    true_iter, false_iter = itertools.tee(items, 2)
    return filter(predicate, true_iter), itertools.filterfalse(predicate, false_iter)


def exhaust(items: Iterable) -> None:
    """Consume all items from an iterator without storing them."""
    for _ in items:
        pass


def parallelize(function: Callable, items: Iterable) -> Iterator:
    """
    Apply the function to items in parallel using multiple processes.

    Results may not be returned in the same order as the input.
    """
    with multiprocessing.Pool() as pool:
        yield from pool.imap_unordered(function, items)


def time_limited_stream(items: Iterable, max_duration: float, delay_per_item: float = 0) -> Iterator:
    """Yield items from the iterable but stop after max_duration seconds."""
    start_time = time.time()
    for item in items:
        if time.time() - start_time > max_duration:
            break
        yield item
        if delay_per_item:
            time.sleep(delay_per_item)


def filter_with_state(items: Iterable, predicate: Callable) -> Iterator:
    """Filter items based on a stateful predicate function."""
    prev = None
    for item in items:
        if prev is None or predicate(prev, item):
            yield item
        prev = item


def rolling_aggregate(items: Iterable, func: Callable, initial=None) -> Iterator:
    """Apply a rolling aggregation to the items."""
    agg = initial
    for item in items:
        agg = func(agg, item) if agg is not None else item
        yield agg


def throttle(items: Iterable, max_rate: int) -> Iterator:
    """
    Limit the rate of items yielded from the iterable.

    Args:
        items (Iterable): The input iterable.
        max_rate (int): Maximum number of items to yield per second.
    """
    interval = 1.0 / max_rate
    for item in items:
        time.sleep(interval)
        yield item


def inner_join(stream1: Iterable, stream2: Iterable, key1: Callable, key2: Callable) -> Iterator:
    """Perform an inner join between two streams based on a key function."""
    lookup = {key2(item): item for item in stream2}
    for item in stream1:
        k = key1(item)
        if k in lookup:
            yield item, lookup[k]
