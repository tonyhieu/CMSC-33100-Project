from Job import Job

class ScheduledJob(Job):

    def __init__(self, job):
        super().__init__(job.id, job.submissionTime, job.intervalLength)
        self.expectedLength = job.expectedLength
        self.segments = []
        self.expectedFinishTime = -1.0

    def addSegment(self, segment):
        self.segments.append(segment)

    def getNumberOfSegments(self):
        return len(self.segments)

    def getFinishTime(self):
        if len(self.segments) < 1:
            raise ValueError("Scheduled Job has no scheduled segments!")
        lastFinishTime = self.segments[0].endTime
        if len(self.segments) > 1:
            for segment in self.segments[1:]:
                lastFinishTime = max(lastFinishTime, segment.endTime)
        return lastFinishTime

    def setExpectedFinishTime(self, expectedFinishTime):
        self.expectedFinishTime = expectedFinishTime