import functools
import itertools
import multiprocessing
from typing import Iterable, Callable, Iterator, Tuple


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
    if iter(items) is items:
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
