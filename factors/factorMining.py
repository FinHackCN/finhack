from gplearn.genetic import SymbolicTransformer›
import pandas as pd
import numpy as np
import gplearn as gp


def _logical(x1,x2,x3,x4):
    return np.where(x1 > x2,x3,x4)
logical = gp.functions.make_function(function = _logical,name = 'logical',arity = 4)

def _boxcox2(x1):
    with np.errstate(over='ignore', under='ignore'):
        return (np.power(x1,2)-1)/2
binary = gp.functions.make_function(function = _binary,name = 'binary',arity = 1)
function_set = ['add', 'sub', 'mul', 'div', 'log', 'sqrt', 'abs', 'neg','inv','sin','cos','tan', 'max', 'min',boxcox2,logical]


gp1 = SymbolicTransformer(generations=1, population_size=1000,
                              hall_of_fame=600, n_components=100,
                              function_set=function_set,
                              parsimony_coefficient=0.0005,
                              max_samples=0.9, verbose=1,
                              random_state=0, n_jobs=3)
 

