

class SchedulePerformance:

    def __init__(self, simulatedJobs, scheduledJobs):

        self.efficiency = -1.0
        self.predictability = -1.0
        self.fairness = -1.0
        self.combined = -1.0
        
        self.calculateEfficiency(simulatedJobs, scheduledJobs)
        self.calculatePredictability(simulatedJobs, scheduledJobs)
        self.calculateFairness(simulatedJobs, scheduledJobs)
        self.calculateCombined(simulatedJobs, scheduledJobs)

    def calculateEfficiency(self, simulatedJobs, scheduledJobs):
        efficiency = 0.0
        for jobID, simulatedJob in enumerate(simulatedJobs):
            scheduledJob = scheduledJobs[jobID]

        self.efficiency = 0.0


    def calculatePredictability(self, simulatedJobs, scheduledJobs):
        self.predictability = 0.0

    def calculateFairness(self, simulatedJobs, scheduledJobs):
        self.fairness = 0.0

    def calculateCombined(self, simulatedJobs, scheduledJobs):
        self.combined = 0.0

    def dump(self):
        print(f"Schedule Performance:")
        print(f"efficiency: {self.efficiency:8.3f}, predictability: {self.predictability:8.3f}")
        print(f"predictability: {self.fairness:8.3f}, combined: {self.combined:8.3f}")
        print()