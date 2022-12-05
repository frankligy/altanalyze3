import os,sys
import pandas as pd
import numpy as np
import h5py


'''
1. hdf5
2. zarr
3. feather
4. parquet
5. pickle
6. plain text

zarr is more for N-dimensional array, feature and parquet is not optimized for retrieving specific part of your data, pickle and txt have clear disadvantages.
references are below:

https://stackoverflow.com/questions/37928794/which-is-faster-for-load-pickle-or-hdf5-in-python
'''



class AltAnalyzeIOHandler(object):
    '''
    The philosophy is you should try to disassemble your dataframe in your RAM to separate parts, including the index,
    column, metadata associated with rows and columns, expression values, and file-level attributes
    '''
    def __init__(self,file_path,mode):
        if mode == 'w':
            if not os.path.exists(file_path):
                f = h5py.File(file_path,'w')
                self.f = f
            else:
                raise Exception('{} already exist, can not write'.format(file_path))
        elif mode == 'r':
            pass
        else:
            raise Exception('mode should be either w or r')

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.f.close()

    def write(self,key,data,dtype=None):
        self.f.create_dataset(key,data,dtype)
        
