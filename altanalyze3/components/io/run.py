import pandas as pd
import numpy as np
import os,sys
sys.path.insert(0,'.')
import io_utils as io
import h5py


# count_matrix = pd.read_csv('/Users/ligk2e/Desktop/tmp/io/counts.TCGA-SKCM.txt',sep='\t',index_col=0)
psi_matrix = pd.read_csv('/Users/ligk2e/Desktop/tmp/io/Hs_RNASeq_top_alt_junctions-PSI_EventAnnotation.txt',sep='\t')
columns = ['Symbol','Description','Examined-Junction','Background-Major-Junction','AltExons','ProteinPredictions','dPSI','ClusterID','UID','Coordinates','EventAnnotation']
key = 'ProteinPredictions'
a = psi_matrix[key]
a.to_csv('test.txt',sep='\t',index=None,header=None);sys.exit('stop')

# with h5py.File('test.h5','w') as f:
#     f.create_dataset('index',data=a.astype('|S25'))


# start to test
with io.AltAnalyzeIOHandler('./test_output.h5','w') as handler:
    handler.write(key=key,data=a,categorical=False,dtype='string')
    handler.view()


