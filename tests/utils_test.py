import os
from datalabframework import utils

def test_lrchop():
    s = utils.lrchop('asd12345aaa','asd','aaa')
    assert(s=='12345')

    s = utils.lrchop('asd12345aaa',b='asd')
    assert(s=='12345aaa')

    s = utils.lrchop('asd12345aaa',e='aaa')
    assert(s=='asd12345')

    s = utils.lrchop('asd12345aaa')
    assert(s=='asd12345aaa')

def test_relative_filename():
    assert(utils.relative_filename('/aaa')=='aaa')

def test_absolute_filename():
    assert(utils.absolute_filename('/aaa')=='/aaa')

    path= os.getcwd()
    assert(utils.absolute_filename('aaa')==path+'/aaa')
