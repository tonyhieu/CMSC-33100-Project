from .Job import Job

class ScheduledJob(Job):

    def __init__(self, job):
        super().__init__(job.id, job.submissionTime, job.nThreads, threads = job.threads)
        self.scheduledSegments = []
        self.expectedFinishTime = -1.0

    def addSegment(self, segment):
        self.scheduledSegments.append(segment)

    def getNumberOfScheduledSegments(self):
        return len(self.scheduledSegments)

    def getFinishTime(self):
        if len(self.scheduledSegments) < 1:
            raise ValueError("Scheduled Job has no scheduled segments!")
        lastFinishTime = self.scheduledSegments[0].endTime
        if len(self.scheduledSegments) > 1:
            for segment in self.scheduledSegments[1:]:
                lastFinishTime = max(lastFinishTime, segment.endTime)
        return lastFinishTime

    def setExpectedFinishTime(self, expectedFinishTime):
        self.expectedFinishTime = expectedFinishTime

    def getTotalRunningTime(self):
        runningTime = 0.0
        for segment in self.scheduledSegments:
            runningTime += segment.endTime - segment.startTime - segment.waitingTime
        return runningTime