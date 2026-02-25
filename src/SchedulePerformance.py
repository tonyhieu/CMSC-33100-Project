

class SchedulePerformance:

    def __init__(self, scheduledJobs):

        self.efficiency = -1.0
        self.predictability = -1.0
        self.fairness = -1.0
        self.combined = -1.0
        
        self.calculateEfficiency(scheduledJobs)
        self.calculatePredictability(scheduledJobs)
        self.calculateFairness(scheduledJobs)
        self.calculateCombined(scheduledJobs)

    def calculateEfficiency(self, scheduledJobs):
        totalWorkingTime = 0.0
        totalWaitingTime = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            totalWaitingTime += scheduledJob.getFinishTime() - scheduledJob.submissionTime
            totalWorkingTime += scheduledJob.intervalLength
        
        print("totalWaitingTime: ", totalWaitingTime)
        print("totalWorkingTime: ", totalWorkingTime)

        self.efficiency = totalWorkingTime / totalWaitingTime


    def calculatePredictability(self, scheduledJobs):

        totalOffset = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            if scheduledJob.expectedFinishTime < 0:
                raise ValueError("scheduledJob expectedFinishTime not set!")
            totalOffset += scheduledJob.getFinishTime() - scheduledJob.expectedFinishTime

        print("totalOffset: ", totalOffset)

        self.predictability = totalOffset / len(scheduledJobs)

    def calculateFairness(self, scheduledJobs):

        totalWaitingTime = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            totalWaitingTime += scheduledJob.getFinishTime() - scheduledJob.submissionTime
        self.fairness = totalWaitingTime / len(scheduledJobs)

    def calculateCombined(self, scheduledJobs):
        self.combined = 0.0

    def dump(self):
        print(f"Schedule Performance:")
        print(f"efficiency: {self.efficiency:8.3f}, predictability: {self.predictability:8.3f}")
        print(f"fairness: {self.fairness:8.3f}, combined: {self.combined:8.3f}")
        print()