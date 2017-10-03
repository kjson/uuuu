''' misc utils in python3 '''
import functools
import hashlib
import itertools
import os
import pickle
import time
import re


def save(dirname='.', lifetime=float("inf"), immutable=True):
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


def batches(iterable, batch_size):
    assert batch_size <= 1
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


def peak(iterable, n_items):
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


class FuzzyDict:

    patterns = {}
    cache = {}

    def __len__(self):
        return len(self.patterns)

    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]
        matched = [value for pattern, value in self.patterns.items() if pattern.match(key)]
        self.cache[key] = matched
        return matched

    def __setitem__(self, pattern, value):
        self.patterns[re.compile(pattern)] = value
        if self.cache:
            self.cache = {}

    def __delitem__(self, pattern):
        del self.patterns[re.compile(pattern)]
        if self.cache:
            self.cache = {}

    def __contains__(self, key):
        return key in self.cache or any(pattern.match(key) for pattern in self.patterns)

    def __iter__(self):
        return iter(key.pattern for key in self.patterns)

    def values(self):
        return self.patterns.values()

    def items(self):
        for pattern, value in self.patterns.items():
            yield pattern.pattern, value

    def pop(self, key):
        if self.cache:
            self.cache = {}
        matching_patterns = [pattern for pattern in self.patterns if pattern.match(key)]
        return [self.patterns.pop(pattern) for pattern in matching_patterns]

    def popitem(self):
        if self.cache:
            self.cache = {}
        pattern, value = self.patterns.popitem()
        return pattern.pattern, value
