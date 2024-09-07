import functools
import itertools
import multiprocessing

from typing import Iterable, Callable, Iterator, Tuple


def batches(items: Iterable, batch_size: int) -> Iterator[Tuple]:
    """Bunches an iterable into batch size tuples."""
    if batch_size < 2:
        raise ValueError("The batch size must be greater than two.")

    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield tuple(batch)
            batch = []

    if batch:
        yield tuple(batch)


def peek(items: Iterable, num_items: int) -> Tuple[Iterator, Iterator]:
    """
    A benign peek to the first @num_items elements of @items. Works on iterators as well.
    """
    if num_items < 0:
        raise ValueError("Cannot peek negative items from an iterable.")

    seq1, seq2 = itertools.tee(items, 2)
    peeked = itertools.islice(seq1, num_items)

    # If the input is an iterator, we want to pass back the peeked and the remaining items.
    if iter(items) is items:
        return peeked, itertools.chain(seq2, items)

    return peeked, seq2


def multimap(functions: Iterable[Callable], items: Iterable) -> Iterator:
    """Applies consecutive functions over the input iterable."""
    return functools.reduce(lambda i, f: map(f, i), functions, items)


def split(predicate: Callable, items: Iterable) -> Tuple[Iterator, Iterator]:
    """Routes items from an iterable into two iterables based on the predicate."""
    true_iter, false_iter = itertools.tee(items, 2)
    return filter(predicate, true_iter), itertools.filterfalse(predicate, false_iter)


def exhaust(items: Iterable) -> None:
    """Drains an iterator. Useful when calling `list` would consume too many resources."""
    for _ in items:
        pass


def parallelize(function: Callable, items: Iterable) -> Iterator:
    """
    Distributes items to as many processes as available and applies the function.
    Results will not necessarily return in order.
    """
    with multiprocessing.Pool() as pool:
        yield from pool.imap_unordered(function, items)
