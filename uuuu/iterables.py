"""A collection of functions that work on a python iterable objects"""
import functools
import itertools


def batches(iterable, batch_size):
    """Bunches an interable into batch size tuples."""
    assert batch_size > 1
    batch = [None] * batch_size
    curret_size = 0
    for item in iterable:
        batch[curret_size] = item
        curret_size += 1
        if curret_size >= batch_size:
            yield tuple(batch)
            curret_size = 0
    if batch and curret_size != 0:
        yield tuple(batch[:curret_size])


def peek(items, num_items):
    """
    An benign peek to the first @num_items elements of @items. Note that this works
    on iterators as well.
    """
    if num_items < 0:
        raise ValueError("Cannot peek negative items from an iterable.")

    seq1, seq2 = itertools.tee(items, 2)
    peeked = itertools.islice(seq1, 0, num_items)

    # In the case that the object is only an iterator, we want to pass back the peek'd iterator
    # as well as the original iterator. So we chain the second sequence with the original
    if items is iter(items):
        return peeked, itertools.chain(seq2, items)

    return peeked, seq2


def multimap(functions, iterable):
    """ Builds an iterable by mapping consecutive function over the input iterable """
    yield from functools.reduce(lambda i, f: map(f, i), functions, iterable)


def split(predicate, iterable):
    """ Route items from an iterable into two new iterable, one where the predicate is true. """
    true, false = itertools.tee(iterable, 2)
    return filter(predicate, true), itertools.filterfalse(predicate, false)


def exhaust(iterable):
    """ Drains an iterator. Useful for times when calling `list` would consume to much resources """
    for _ in iterable:
        pass
