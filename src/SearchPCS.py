#!/usr/bin/env python


import inspect
from src.PCSSearchAlgo import PCSParameterSearch
from pymoo.core.problem import Problem
import pickle
from argparse import ArgumentParser

def SearchPCSParameters():
    parser = ArgumentParser(description="Find Pareto Optimal Frontier of Parameters for WFQ")
    parser.add_argument("-i", "--input", 
                              type=str,
                              default = "jobs.pkl",
                              help="Input file name")
    
    parser.add_argument("-s", "--plot",
                              action="store_true",
                              help="plot the pareto frontier")
    
    parser.add_argument("-g", "--ngen",
                              type=int,
                              default = "50",
                              help="number of generations to run the search")
    
    parser.add_argument("-p", "--population",
                              type=int,
                              default = "40",
                              help="population in each generation of the search")

    args = parser.parse_args()
    with open(args.input, "rb") as f:
        jobList, globalSemaphoreList = pickle.load(f)
        
    problem = PCSParameterSearch(jobList=jobList, 
                                 globalSemaphoreList=globalSemaphoreList)
    results = problem.findParetoFrontier(pop_size=args.population, n_gen=args.ngen)
    print(results[0])
    if args.plot:
        problem.plotParetoFrontier()

if __name__ == "__main__":
    SearchPCSParameters()

