''' misc utils in python3 '''
import functools
import hashlib
import itertools
import os
import pickle
import time


def filecache(dirname: str = '.', lifetime: float = float("inf"), immutable: bool = True):
    '''
    Persists the decorated functions return value to the filesystem.

    @dirname: Where to save the files.
    @lifetime: How old ( in seconds) the persisted file can be before recompute.
    @immutable: The persisted file must match the decorated func in cpython bytecode.

    '''
    def outer(func):
        info = func.__name__ + str(func.__code__.co_code) if immutable else func.__name__
        filename = os.path.join(dirname, hashlib.sha224(info.encode('UTF-8')).hexdigest())
        @functools.wraps(func)
        def inner(*args, **kwargs):
            if os.path.isfile(filename) and time.time() - os.path.getmtime(filename) < lifetime:
                with open(filename, 'rb') as pickle_file:
                    data = pickle.load(pickle_file)
            else:
                with open(filename, 'wb') as pickle_file:
                    data = func(*args, **kwargs)
                    pickle.dump(data, pickle_file)
            return data
        return inner
    return outer


def batches(iterable, batch_size: int):
    ''' Yield tuples of @batch_size length from @iterable. '''
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


def peak(iterable, n_items: int = 1):
    ''' Peak the first @n_items elements of @iterable. '''
    seq1, seq2 = itertools.tee(iterable, 2)
    return itertools.islice(seq1, 0, n_items), itertools.chain(seq2, iterable)


def multimap(functions, iterable):
    yield from functools.reduce(lambda i, f: map(f, i), functions, iterable)


def split(predicate, iterable):
    true, false = itertools.tee(iterable, 2)
    return filter(predicate, true), itertools.filterfalse(predicate, false)


def exhaust(iterable):
    for _ in iterable:
        pass
