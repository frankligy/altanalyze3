#!/data/salomonis2/LabFiles/Frank-Li/refactor/neo_env/bin/python3.7

import os
import sys
import pandas as pd
import numpy as np
from copy import deepcopy
import argparse


def find_uid_in_clique(the_uid,region,strand,uid2coords):
    clique = {the_uid:region}
    for uid,coords in uid2coords.items():
        if uid != the_uid:
            if strand == '+':
                cond = (max(int(region[0]),int(coords[0])) - min(int(region[1]),int(coords[1]))) < 0
                if cond:  # they overlap
                    clique[uid] = coords
            elif strand == '-':
                cond = (max(int(region[1]),int(coords[1])) - min(int(region[0]),int(coords[0]))) < 0
                if cond: # they overlap
                    clique[uid] = coords
    return clique



def is_valid(mat,uid):
    num_incl_events = np.count_nonzero(mat[0,:] >= 20)
    num_excl_events = np.count_nonzero(mat[1:,:].sum(axis=0) >= 20)
    total_number_junctions = np.count_nonzero(mat.sum(axis=0) >= 20)
    max_incl_exp = mat[0,:].max()
    max_excl_exp = mat[1:,:].sum(axis=0).max()
    ratio = max_excl_exp / max_incl_exp
    return (num_incl_events > 1 and num_excl_events > 1 and total_number_junctions > 2 and ratio > 0.1)


def calculate_psi_core(clique,uid,count,sample_columns):
    sub_count = count.loc[list(clique.keys()),sample_columns]
    mat = sub_count.values
    psi = mat[0,:] / mat.sum(axis=0)
    if sub_count.shape[0] > 1:
        bg_uid = sub_count.index.tolist()[np.argmax(mat[1:,:].sum(axis=1)) + 1]
        cond = is_valid(mat,uid)
    else:  # no background event
        bg_uid = 'None'      
        cond = False
    return psi,bg_uid,cond,sub_count.iloc[1:,:].to_dict()
        


def calculate_psi_per_gene(count,outdir):
    return_data = []
    count = count.set_index('uid')
    uid2coords = count.apply(lambda x:[x['start'],x['end']],axis=1,result_type='reduce').to_dict()
    for row in count.iterrows():
        uid = row[0]
        region = uid2coords[uid]
        strand = row[1]['strand']
        clique = find_uid_in_clique(uid,region,strand,uid2coords)
        index_list = deepcopy(row[1].index.tolist())
        for c in ['gene','chrom','start','end','strand']:
            index_list.remove(c)
        sample_columns = index_list
        psi_values,bg_uid, cond, dic = calculate_psi_core(clique,uid,count,sample_columns)
        data = (uid,bg_uid,cond,dic,*psi_values)
        return_data.append(data)
    df = pd.DataFrame.from_records(data=return_data,columns=['uid','bg_uid','cond','dic']+sample_columns)
    df.to_csv(os.path.join(outdir,'output_psi.txt'),sep='\t',index=None)

        


        
def main(args):
    junction_path = args.junction
    query_gene = args.gene
    outdir = args.outdir
    
    count = pd.read_csv(junction_path,sep='\t',index_col=0)
    col_uid = []
    col_gene = []
    col_chrom = []
    col_start = []  # logical start and end, not physical on the forward strand
    col_end = []
    col_strand = []
    for item in count.index:
        uid,coords = item.split('=')
        chrom, coords = coords.split(':')
        start, end = coords.split('-')
        strand = '+' if start < end else '-'
        gene = uid.split(':')[0]
        col_uid.append(uid)
        col_gene.append(gene)
        col_chrom.append(chrom)
        col_start.append(start)
        col_end.append(end)
        col_strand.append(strand)
    for name,col in zip(['uid','gene','chrom','start','end','strand'],[col_uid,col_gene,col_chrom,col_start,col_end,col_strand]):
        count[name] = col
    test_count = count.loc[count['gene']==query_gene,:]
    calculate_psi_per_gene(test_count,outdir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute PSI from count junction')
    parser.add_argument('--junction',type=str,default=None,help='the path to the junction count')
    parser.add_argument('--gene',type=str,default=None,help='gene you want to compute PSI')
    parser.add_argument('--outdir',type=str,default=None,help='output dir for the output file')
    args = parser.parse_args()
    main(args)

