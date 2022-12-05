import pandas as pd
import numpy as np
import os,sys
sys.path.insert(0,'.')
import io_utils as io
import h5py


# count_matrix = pd.read_csv('/Users/ligk2e/Desktop/tmp/io/counts.TCGA-SKCM.txt',sep='\t',index_col=0)
psi_matrix = pd.read_csv('/Users/ligk2e/Desktop/tmp/io/Hs_RNASeq_top_alt_junctions-PSI_EventAnnotation.txt',sep='\t',index_col=0)
a = psi_matrix.index.values
print(a.dtype.kind)

# with h5py.File('test.h5','w') as f:
#     f.create_dataset('index',data=a.astype('|S25'))


# start to test
with io.AltAnalyzeIOHandler('./test_output.h5','w') as handler:
    handler.custom_write(key='index',data=a,categorical=None,dtype=None)
    handler.view()


