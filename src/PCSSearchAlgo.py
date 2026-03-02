import numpy as np
import pickle
from pymoo.core.problem import Problem
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.optimize import minimize
import matplotlib.pyplot as plt

from .AlgoPCS import AlgoPCS
from .Scheduler import Scheduler
    
class PCSParameterSearch(Problem):

    def __init__(self, jobList=None, globalSemaphoreList=None):
        
        # Could search over threshholds as well, would slow it down a ton though
        # Perhaps some way of parameterizing thresholds (?) but seems clunky
        super().__init__(
            n_var=2,         # nQueues, W
            n_obj=2,         # avg JCT, predictability
            n_constr=0,      # no constraints
            # More intelligent selection of bounds
            xl=np.array([1, 0]),  # lower bounds
            xu=np.array([10, 5])      # upper bounds
        )
        self.jobList = jobList
        self.globalSemaphoreList = globalSemaphoreList
        self.paretoFroniter = None

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
        algo = AlgoPCS(len(self.jobList), 
                   self.globalSemaphoreList, 
                   nQueues=nQueues, 
                   W=W,
                    thresholds=thresholds)
        scheduler = Scheduler(algo, self.jobList)
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
    def plotParetoFrontier(self):
        if self.paretoFroniter is None:
            raise ValueError("Frontier Not Calculated")
        plt.scatter(self.paretoFroniter[:,0], self.paretoFroniter[:,1])
        plt.xlabel("JCT")
        plt.ylabel("Predictability")
        plt.title("Pareto Frontier")
        plt.show()
