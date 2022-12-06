import os,sys
import pandas as pd
import numpy as np
import h5py
import bisect


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
            f = h5py.File(file_path,'r')
        else:
            raise Exception('mode should be either w or r')

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.f.close()

    @staticmethod
    def compute_optimal_fixed_length(data):
        # if executing this function, then dtype must be string
        max_length = max([len(item) for item in data])
        return max_length

    @staticmethod
    def replace_nan(data,dtype):
        if dtype == 'string':
            data = np.array(['no_content' if item is np.nan else item for item in data])
        elif dtype == 'number':
            data = np.nan_to_num(data,nan=-1)
        return data

    @staticmethod
    def compute_n_byte_needed(categories):
        n = len(categories)
        if n <= 2**(1*8):
            n_byte_needed = 1
        elif n > 2**(1*8) and n <= 2**(2*8):
            n_byte_needed = 2
        elif n > 2**(2*8) and n <= 2**(4*8):
            n_byte_needed = 4
        elif n > 2**(4*8) and n <= 2**(8*8):
            n_byte_needed = 8
        else:
            raise Exception('number of categories can not be encoded using 2**64, reconsider categorical argument')
        return n_byte_needed


    def write(self,key,data,categorical,dtype):
        data = AltAnalyzeIOHandler.replace_nan(data,dtype)
        if dtype == 'string':
            if categorical:
                category2code = {}
                categories = []
                for i,v in enumerate(np.unique(data)):  # np unique guarantee to be in ascending lexicographic order
                    category2code[v] = i
                    categories.append(v)
                codes = np.array([category2code[item] for item in data])
                categories = np.array(categories)
                n_byte_needed = AltAnalyzeIOHandler.compute_n_byte_needed(categories)

                group = self.f.create_group(key)
                group.create_dataset('codes',data=codes.astype('|i{}'.format(n_byte_needed)))  # |i1 = int8, |i2 = int16, |i3 = int32, |i4 = int64
                optimal_fixed_length = AltAnalyzeIOHandler.compute_optimal_fixed_length(data)
                dtype = '|S{}'.format(optimal_fixed_length)
                group.create_dataset('categories',data=categories.astype(dtype))

            else:
                optimal_fixed_length = AltAnalyzeIOHandler.compute_optimal_fixed_length(data)
                dtype = '|S{}'.format(optimal_fixed_length)
                self.f.create_dataset(key,data=data)


    def view(self):
        # self.f.visititems(print)
        def print_item(k, v):  # recursive function
            if isinstance(v, h5py.Dataset):
                print(k,v)
            elif isinstance(v, h5py.Group):
                print(k,v)
                for vk, vv in v.items():
                    print_item(vk, vv)

        for k,v in self.f.items():
            print_item(k,v)

        
            

        
