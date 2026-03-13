import numpy as np
import pickle
from pymoo.core.problem import Problem
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.optimize import minimize
import matplotlib.pyplot as plt

from .AlgoPCS import AlgoPCS
from .Scheduler import Scheduler
    
class PCSParameterSearch(Problem):

    def __init__(self, jobList=None, globalSemaphoreList=None, nCores=8):
        
        self.nCores = nCores 
        self.jobList = jobList
        self.globalSemaphoreList = globalSemaphoreList
        self.paretoFroniter = None

        super().__init__(
            n_var=2,         # nQueues, W
            n_obj=2,         # avg JCT, predictability
            n_constr=0,      # no constraints
            xl=np.array([1, 0.0]),  
            xu=np.array([nCores - 1, 5.0]) 
        )

    # Evaluate performance of each parameter selection for search
    # Will probably parralelize later
    def _evaluate(self, X, out, *args, **kwargs):
        results = []

        for row in X:
            nQueues, W = row
            print("Trying: nQueues =", int(round(nQueues)), "W =", W)
            AvgJCT, pred = self.calculatePCSPerformance(int(round(nQueues)), W, None)
            results.append([AvgJCT, pred])

        out["F"] = np.array(results)
    
    # Calculate performance performance of PCS schedule
    def calculatePCSPerformance(self, nQueues, W, thresholds):
        import copy
        job_copy = copy.deepcopy(self.jobList)
        semaphore_copy = copy.deepcopy(self.globalSemaphoreList)
        algo = AlgoPCS(self.nCores, 
                   semaphore_copy, 
                   nQueues=nQueues, 
                   W=W,
                    thresholds=thresholds)
        scheduler = Scheduler(algo, job_copy)
        scheduler.createSchedule()
        schedulePerformance = scheduler.evaluateSchedule(verbose=False)
        return schedulePerformance.AvgJCT, schedulePerformance.predictability
    
    # Find the pareto frontier with optimal values
    def findParetoFrontier(self, pop_size = 40, n_gen = 50, verbose = False):
        algorithm = NSGA2(pop_size=pop_size)
        res = minimize(
            self,
            algorithm,
            ('n_gen', n_gen),
            seed=1,
            verbose=verbose   
        )
        self.paretoFroniter = res.F
        return res.X, res.F
    
    # Plot the pareto frontier found above
    def plotParetoFrontier(self, save_path=None):
        if self.paretoFroniter is None:
            raise ValueError("Frontier Not Calculated")
        plt.figure()
        plt.scatter(self.paretoFroniter[:,0], self.paretoFroniter[:,1])
        plt.xlabel("JCT")
        plt.ylabel("Predictability")
        plt.title("Pareto Frontier")
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def saveParetoCSV(self, X, F, save_path):
        import csv
        with open(save_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["nQueues", "W", "AvgJCT", "Predictability"])
            for params, metrics in zip(X, F):
                writer.writerow([int(round(params[0])), params[1], metrics[0], metrics[1]])
