import random
from .Job import Job

class SimulatedJob(Job):

    def __init__(self, jobID, submissionTime, nThreads, expectedLengths, lengthUncertainty):
        actualLengths = []
        for expectedLength in expectedLengths:
            #my first thought to sample only jobs with positive definite length, can probably be improved
            actualLength = 0.0
            while actualLength <= 0:
                actualLength = expectedLength + random.gauss(mu=0.0, sigma=lengthUncertainty)
            actualLengths.append(actualLength)
        super().__init__(jobID,submissionTime, nThreads, actualLengths=actualLengths, expectedLengths=expectedLengths)
