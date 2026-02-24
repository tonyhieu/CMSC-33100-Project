import random
from src import Job

class SimulatedJob(Job.Job):

    def __init__(self, jobID, submissionTime, expectedLength, lengthUncertainty):
        self.expectedLength = expectedLength
        self.lengthUncertainty = lengthUncertainty

        #my first thought to sample only jobs with positive definite length, can probably be improved
        actualLength = 0.0
        while actualLength <= 0:
            actualLength = self.expectedLength + random.gauss(mu=0.0, sigma=self.lengthUncertainty)
        super().__init__(jobID,submissionTime, actualLength)

    def dump(self):
        super().dump()
        print(f"-------------Expected Length: {self.expectedLength:8.3f}")
        print()
