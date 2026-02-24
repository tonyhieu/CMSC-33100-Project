#!/usr/bin/env python
import SimulatedJob
import numpy as np
import pickle

def simulateJobs():
    outputFileName = "jobs.pkl"

    n = 100
    maxSubmissionTime = 1000.
    averageJobLength = 10.
    jobLengthUncertainty = 5.

    # SORT the submission times so they looping through them submits the jobs in order
    sampledSubmissionTimes = np.random.uniform(low=0.0, high=maxSubmissionTime, size=n)
    sampledSubmissionTimes.sort()
    sampledIntervalLengths = samples = np.random.poisson(lam=averageJobLength, size=n)
    jobsList = []

    for i in range(n):
        newJob = SimulatedJob.SimulatedJob(sampledSubmissionTimes[i],
                                           sampledIntervalLengths[i],
                                           jobLengthUncertainty)
        newJob.dump()
        jobsList.append(newJob)

    import pickle

    with open(outputFileName, "wb") as f:
        pickle.dump(jobsList, f)

if __name__ == "__main__":
    simulateJobs()