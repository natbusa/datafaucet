from datafaucet import logging

def foo():
    logging.info('foo')
    bar()

def bar():
    logging.info('bar')
    
if __name__ == '__main__':
    foo()