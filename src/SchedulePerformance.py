
BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

class SchedulePerformance:

    def __init__(self, scheduledJobs, algo, schedule=None):

        self.algo = algo
        self.schedule = schedule
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
            totalWorkingTime += scheduledJob.getTotalRunningTime()
        
        print("totalWaitingTime: ", totalWaitingTime)
        print("totalWorkingTime: ", totalWorkingTime)

        self.efficiency = totalWorkingTime / totalWaitingTime


    def calculatePredictability(self, scheduledJobs):

        totalOffset = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            if scheduledJob.expectedFinishTime < 0:
                print(jobID, scheduledJob.expectedFinishTime)
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

    def visualize(self, width=80):
        if self.schedule is None:
            print("No schedule available for visualization.")
            return

        min_time = float('inf')
        max_time = 0.0
        for coreID in range(self.schedule.nCores):
            if self.schedule.schedule[coreID]:
                min_time = min(min_time, self.schedule.schedule[coreID][0].startTime)
                max_time = max(max_time, self.schedule.schedule[coreID][-1].endTime)
        if min_time == float('inf'):
            min_time = 0.0
        total_time = max_time - min_time
        if total_time == 0:
            return

        print("\nSchedule Visualization:")
        for coreID in range(self.schedule.nCores):
            timeline = ['-'] * width
            for segment in self.schedule.schedule[coreID]:
                start_pos = int((segment.startTime - min_time) / total_time * width)
                end_pos = int((segment.endTime - min_time) / total_time * width)
                end_pos = max(start_pos + 1, end_pos)
                end_pos = min(end_pos, width)
                char = BASE64_CHARS[segment.jobID % 64]
                for pos in range(start_pos, end_pos):
                    timeline[pos] = char
            print(f"Core {coreID + 1:<3} | {''.join(timeline)}")
        print()

    def dump(self):
        print(f"{self.algo} Schedule Performance:")
        print(f"efficiency: {self.efficiency:8.3f}, predictability: {self.predictability:8.3f}")
        print(f"fairness: {self.fairness:8.3f}, combined: {self.combined:8.3f}")
        print()