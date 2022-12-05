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
            f = h5py.File(file_path,'r')
        else:
            raise Exception('mode should be either w or r')

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.f.close()

    @staticmethod
    def compute_optimal_fixed_length(data):
        max_length = max([len(item) for item in data])
        return max_length

    def custom_write(self,key,data,categorical=None,dtype=None):
        if categorical is None:
            if len(np.unique(data)) < 2**16:  # can be encoded using int16
                categorical = True
            else:
                categorical = False

        if categorical:
            category2code = {}
            categories = []
            for i,v in enumerate(np.unique(data)):  # np unique guarantee to be in ascending lexicographic order
                category2code[v] = i
                categories.append(v)
            codes = np.array([category2code[item] for item in data])
            categories = np.array(categories)

            group = self.f.create_group(key)
            group.create_dataset('codes',data=codes.astype('|i2'))  # |i1 = int8, |i2 = int16
            if dtype is None:
                auto_inferred_dtype = data.dtype
                if auto_inferred_dtype.kind == 'O':  # numpy Object
                    optimal_fixed_length = AltAnalyzeIOHandler.compute_optimal_fixed_length(data)
                    dtype = '|S{}'.format(optimal_fixed_length)
            group.create_dataset('categories',data=categories.astype(dtype))

        else:
            if dtype is None:
                auto_inferred_dtype = data.dtype
                if auto_inferred_dtype.kind == 'O':  # numpy Object
                    optimal_fixed_length = AltAnalyzeIOHandler.compute_optimal_fixed_length(data)
                    dtype = '|S{}'.format(optimal_fixed_length)
            self.f.create_dataset(key,data=data.astype(dtype))


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

        
            

        
