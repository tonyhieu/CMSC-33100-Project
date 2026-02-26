#!/usr/bin/env python
from src.SimulatedJob import SimulatedJob
import numpy as np
import pickle
import argparse

def simulateJobs():
    parser = argparse.ArgumentParser(description="Create list of simulated jobs")
    parser.add_argument("-o", "--output", 
                              type=str,
                              default = "jobs.pkl",
                              help="Output file name")
    parser.add_argument("-n", "--number", 
                              type=int, 
                              default = 100, 
                              help="number of jobs to simulate")
    parser.add_argument("-t", "--time", 
                              type=float, 
                              default = 1000., 
                              help="latest possible sumbission time")
    parser.add_argument("-l", "--length", 
                              type=float, 
                              default = 10., 
                              help="average job length")
    parser.add_argument("-u", "--uncertainty", 
                              type=float, 
                              default = 5., 
                              help="unvertainty in each jobs length")

    args = parser.parse_args()

    outputFileName = args.output

    n = args.number
    maxSubmissionTime = args.time
    averageJobLength = args.length
    jobLengthUncertainty = args.uncertainty

    # SORT the submission times so they looping through them submits the jobs in order
    sampledSubmissionTimes = np.random.uniform(low=0.0, high=maxSubmissionTime, size=n)
    sampledSubmissionTimes.sort()
    sampledIntervalLengths = np.random.poisson(lam=averageJobLength, size=n)
    jobsList = []

    for i in range(n):
        newJob = SimulatedJob(i, 
                                           sampledSubmissionTimes[i],
                                           sampledIntervalLengths[i],
                                           jobLengthUncertainty)
        newJob.dump()
        jobsList.append(newJob)

    with open(outputFileName, "wb") as f:
        pickle.dump(jobsList, f)

if __name__ == "__main__":
    simulateJobs()