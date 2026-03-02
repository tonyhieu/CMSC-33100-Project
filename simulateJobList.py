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
                              help="uncertainty in each jobs length")
    parser.add_argument("--threads", 
                            type=float, 
                            default = 3., 
                            help="average number of threads for each job")
    parser.add_argument("--mut", 
                            type=float, 
                            default = 0., 
                            help="probability of having a mutex in a given job")
    parser.add_argument("--sem", 
                            type=float, 
                            default = 0., 
                            help="probability of having a semaphore")

    args = parser.parse_args()

    outputFileName = args.output

    n = args.number
    maxSubmissionTime = args.time
    averageJobLength = args.length
    jobLengthUncertainty = args.uncertainty
    averageThreadNumber = args.threads

    # SORT the submission times so they looping through them submits the jobs in order
    sampledSubmissionTimes = np.random.uniform(low=0.0, high=maxSubmissionTime, size=n)
    sampledSubmissionTimes.sort()
    sampledThreadNumber = np.random.poisson(lam=averageThreadNumber - 1, size=n) + 1
    jobsList = []
    globalSemaphoreList = []
    jobsThreadLengths = np.exp(np.random.normal(loc=np.log(averageJobLength), scale=np.log(jobLengthUncertainty) + 2, size=n))
    if np.any(jobsThreadLengths <= 0):
        raise ValueError("No negative lengths!")
    for i in range(n):
        #sample one job length per thread
        sampledIntervalLengths = np.random.poisson(lam=jobsThreadLengths[i], size=sampledThreadNumber[i])
        newJob = SimulatedJob(i, 
                              sampledSubmissionTimes[i],
                              sampledThreadNumber[i],
                              sampledIntervalLengths,
                              jobLengthUncertainty,
                              args.sem,
                              args.mut,
                              globalSemaphoreList)
        newJob.dump()
        jobsList.append(newJob)

    with open(outputFileName, "wb") as f:
        pickle.dump((jobsList, globalSemaphoreList), f)

if __name__ == "__main__":
    simulateJobs()