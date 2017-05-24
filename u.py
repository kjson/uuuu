''' misc utils in python3 '''
import functools
import hashlib
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
