from src import Job

class ScheduledJob(Job.Job):

    def __init__(self, job, expectedFinishTime):
        super().__init__(job.id, job.submissionTime, job.intervalLength)
        self.expectedFinishTime = expectedFinishTime
        self.segments = []

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