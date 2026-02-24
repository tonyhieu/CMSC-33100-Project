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