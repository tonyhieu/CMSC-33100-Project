from .Segment import Segment

class Schedule:

    def __init__(self, nCores):
        self.nCores = nCores
        self.schedule = [[] for _ in range(self.nCores)]

    def getExactEndTime(self, coreID):
        if len(self.schedule[coreID]) > 0:
            return self.schedule[coreID][-1].endTime
        else:
             return 0.0

    def getLastJobsExpectedEndTime(self, coreID):
        if len(self.schedule[coreID]) > 0:
            return self.schedule[coreID][-1].startTime + self.schedule[coreID][-1].expectedDuration
        else:
             return 0.0

    def addSegment(self, segment):
        self.schedule[segment.coreID].append(segment)
        return len(self.schedule[segment.coreID]) - 1

    def trimRunningSegment(self, time):
        if time < self.schedule[-1].startTime:
            raise ValueError("Current Time is before running segment's start time")
        elif time > self.schedule[-1].endTime:
            raise ValueError("Current Time is after running segment's end time")
        runningSegment = self.schedule[-1]

        #this is the part of the segment that already finished running
        trimmedSegment = Segment(runningSegment.segmentID,
                                         runningSegment.startTime,
                                         time,
                                         runningSegment.jobID,
                                         time - runningSegment.startTime)
        self.schedule[-1] = trimmedSegment
        return runningSegment

    def dump(self):
        for coreID in range(self.nCores):
            print(f"Schedule on Core {coreID:5}:")
            for j, segment in enumerate(self.schedule[coreID]):
                print(f"\tSegment {j:5} in Schedule:")
                segment.dump()