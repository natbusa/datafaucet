import os, json, base64, binascii

from collections import namedtuple
import copy

def config_fromdict(arg_dict, varname='NB_PARAMS', encode=None):
    try:
        pdata = os.getenv(varname)
    
        if pdata:
            if encode=='base64':
                pdata = base64.b64decode(pdata)
                pdata = pdata.decode('utf-8')
            else:
                pass
        else:
            pdata = '{}'
        
        params = json.loads(pdata)
        
    except json.JSONDecodeError as e:
        print('Invalid json in env variable {}'.format(varname))
        raise(e)
    except binascii.Error as e:
        print('Invalid base64 encoding in env variable {}'.format(varname))
        raise(e)
    except UnicodeDecodeError as e:
        print('Invalid utf-8 encoding in env variable {}'.format(varname))
        raise(e)

    # update the args
    arg_dict.update(params)
    
    #return the updated arg_dict as a named tuple
    obj = namedtuple('GenericDict', arg_dict.keys())(**arg_dict)
        
    return obj