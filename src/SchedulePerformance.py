
import math


BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

class SchedulePerformance:

    def __init__(self, scheduledJobs, algo, schedule=None, verbose=False, ):

        self.algo = algo
        self.schedule = schedule
        self.verbose = verbose
        self.efficiency = -1.0
        self.predictability = -1.0
        self.fairness = -1.0
        self.combined = -1.0
        self.AvgJCT = -1.0
        
        self.calculateAvgJCT(scheduledJobs)
        self.calculatePredictability(scheduledJobs)
        self.calculateEfficiency(scheduledJobs)
        self.calculateFairness(scheduledJobs)
        self.calculateCombined(scheduledJobs)

    def calculateEfficiency(self, scheduledJobs):
        totalWorkingTime = 0.0
        totalWaitingTime = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            totalWaitingTime += scheduledJob.getFinishTime() - scheduledJob.submissionTime
            totalWorkingTime += scheduledJob.getTotalRunningTime()
        
        if self.verbose:
            print("totalWaitingTime: ", totalWaitingTime)
            print("totalWorkingTime: ", totalWorkingTime)

        if totalWaitingTime <= 0.0:
            self.efficiency = 0.0
        else:
            self.efficiency = totalWorkingTime / totalWaitingTime

    def calculateAvgJCT(self, scheduledJobs):
        if len(scheduledJobs) == 0:
            self.AvgJCT = 0.0
            return

        jobCompletionTime = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            jobCompletionTime += scheduledJob.getFinishTime() - scheduledJob.submissionTime
        
        self.AvgJCT = jobCompletionTime / len(scheduledJobs)


    def calculatePredictability(self, scheduledJobs):
        if len(scheduledJobs) == 0:
            self.predictability = 0.0
            return

        totalOffset = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            if scheduledJob.expectedFinishTime < 0:
                print(jobID, scheduledJob.expectedFinishTime)
                raise ValueError("scheduledJob expectedFinishTime not set!")
            totalOffset += abs(scheduledJob.getFinishTime() - scheduledJob.expectedFinishTime)

        if self.verbose:
            print("totalOffset: ", totalOffset)

        self.predictability = totalOffset / len(scheduledJobs)

    def calculateFairness(self, scheduledJobs):
        if len(scheduledJobs) == 0:
            self.fairness = 0.0
            return

        self.fairness = 0.0
        for jobID, scheduledJob in scheduledJobs.items():
            totalSegmentComputeTime = 0.0
            coresUsed = []
            for segmentID, segment in enumerate(scheduledJob.scheduledSegments):
                # check if this segment actually ran or was preeempted
                if segment.finishedRunning:
                    if not (segment.coreID in coresUsed):
                        coresUsed.append(segment.coreID)
                    totalSegmentComputeTime += segment.endTime - segment.startTime
            if len(coresUsed) == 0:
                averageSegmentComputeTime = 0.0
            else:
                averageSegmentComputeTime = totalSegmentComputeTime / len(coresUsed)  
                
            jobFinishTime = scheduledJob.getFinishTime()
            
            # Clamp to 0.0 to prevent floating-point micro-negatives from crashing the sim
            delay = jobFinishTime - scheduledJob.submissionTime - averageSegmentComputeTime
            self.fairness = max(self.fairness, max(0.0, delay))
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

        print(f"efficiency (higher is optimal): {self.efficiency:8.3f}")
        print(f"predictability (lower is optimal): {self.predictability:8.3f}")
        print(f"fairness (lower is optimal): {self.fairness:8.3f}")
        print(f"AvgJCT (lower is optimal): {self.AvgJCT:8.3f}")
        print()