
class Schedule:

    def __init__(self):
        self.schedule = []

    def getCurrentEndTime(self):
        if len(self.schedule) > 0:
            return self.schedule[-1].endTime
        else:
             return 0.0
    
    def getExpectedEndTimeAtSubmission(self, currentTime):
        if len(self.schedule) == 0:
            return 0.0
        endOfMostRecentSegment = self.getCurrentEndTime()
        expectedDurationOfFutureSegments = 0.0
        processingSegment = len(self.schedule) - 1
        while endOfMostRecentSegment > currentTime:
            expectedDurationOfFutureSegments += self.schedule[processingSegment].expectedDuration
            processingSegment -= 1
            if processingSegment < 0:
                endOfMostRecentSegment = 0.0
                break
            else:
                endOfMostRecentSegment = self.schedule[processingSegment].endTime

        return endOfMostRecentSegment + expectedDurationOfFutureSegments

    def addSegment(self, segment):
        self.schedule.append(segment)
        return len(self.schedule) - 1

    def replaceLastSegment(self, segment):
        lastSegment = self.schedule[-1]
        self.schedule[-1] = segment
        return lastSegment

    def dump(self):
        for i, segment in enumerate(self.schedule):
            print(f"Segment {i:5} in Schedule:")
            segment.dump()