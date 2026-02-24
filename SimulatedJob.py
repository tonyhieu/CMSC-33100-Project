import random

class SimulatedJob:

    def __init__(self, submissionTime, expectedLength, lengthUncertainty):
        self.submissionTime = submissionTime
        self.expectedLength = expectedLength
        self.lengthUncertainty = lengthUncertainty

        #my first thought to sample only jobs with positive definite length, can probably be improved
        self.actualLength = 0.0
        while self.actualLength <= 0:
            self.actualLength = self.expectedLength + random.gauss(mu=0.0, sigma=lengthUncertainty)

    def dump(self):
        print(f"-------Job Submitted at time: {self.submissionTime:8.3f}------------")
        print(f"Expected Length: {self.expectedLength:8.3f}, Actual Length: {self.actualLength:8.3f}")
        print()
