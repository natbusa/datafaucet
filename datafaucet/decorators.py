"""
# how methods to classes
# and attributes to fucntions

Example:

class A:
    def __init__(self,v=None):
        self.v = v

@add_method(A)
def incr(self):
    self.v += 1
    return self.v

def foo(self):
    return foo

@add_attr(foo)
def bar():
    print('bar')

@add_attr(foo)
def baz():
    print('baz')

A.foo = property(foo)

a = A(2)
a.incr()
3

a.foo.bar()
'bar'

"""

from functools import wraps


def add_method(cls):
    return add_attr(cls)


def add_attr(cls):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            f = func(*args, **kwargs)
            return f

        setattr(cls, func.__name__, wrapper)

    return decorator
