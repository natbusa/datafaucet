import os

def rootpath():
    # start from here and go up
    # till you find a main.ipynb
    
    path = '.'
    
    while True:
    
        try:
            ls = os.listdir(path)
            if 'main.ipynb' in ls:
                return os.path.abspath(path)

            # we should still be inside a python (hierarchical) package
            # if not, we have gone too far
            if '__init__.py' not in ls:
                return None
        except:
            #bailing out
            return None
    
        path += '/..'
    