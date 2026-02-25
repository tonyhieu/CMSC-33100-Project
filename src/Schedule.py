from src import Segment

class Schedule:

    def __init__(self):
        self.schedule = []

    def getExactEndTime(self):
        if len(self.schedule) > 0:
            return self.schedule[-1].endTime
        else:
             return 0.0

    def getLastJobsExpectedEndTime(self):
        if len(self.schedule) > 0:
            return self.schedule[-1].startTime + self.schedule[-1].expectedDuration
        else:
             return 0.0

    def addSegment(self, segment):
        self.schedule.append(segment)
        return len(self.schedule) - 1

    def trimRunningSegment(self, time):
        if time < self.schedule[-1].startTime:
            raise ValueError("Current Time is before running segment's start time")
        elif time > self.schedule[-1].endTime:
            raise ValueError("Current Time is after running segment's end time")
        runningSegment = self.schedule[-1]

        #this is the part of the segment that already finished running
        trimmedSegment = Segment.Segment(runningSegment.segmentID,
                                         runningSegment.startTime,
                                         time,
                                         runningSegment.jobID,
                                         time - runningSegment.startTime)
        self.schedule[-1] = trimmedSegment
        return runningSegment

    def dump(self):
        for i, segment in enumerate(self.schedule):
            print(f"Segment {i:5} in Schedule:")
            segment.dump()