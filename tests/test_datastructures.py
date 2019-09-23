from datafaucet._utils import merge

def test_merge():
    a = {'a': 1, 'b': 4, 'c': {'merge1': 2}}
    b = {'d': 'add', 'b': 'override', 'c': {'merge2': 4}}
    r1 = merge(a, b)
    r2 = {'a': 1, 'd': 'add', 'b': 'override', 'c': {'merge2': 4, 'merge1': 2}}
    assert (r1 == r2)
