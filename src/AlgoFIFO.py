from src import AlgoBase
from src import SchedulePerformance
from src import ScheduledJob
from src import Segment

class AlgoFIFO(AlgoBase.AlgoBase):

    def __init__(self):
        super().__init__("FIFO")

    def addJobToSchedule(self, job):
        jobStartTime = self.currentSchedule.getCurrentEndTime()
        jobEndTime = jobStartTime + job.intervalLength

        scheduledJob = ScheduledJob.ScheduledJob(job, self.currentSchedule.getExpectedEndTimeAtSubmission(job.submissionTime) + job.expectedLength)
        segmentID = scheduledJob.getNumberOfSegments()


        segment = Segment.Segment(segmentID, jobStartTime, jobEndTime, job.id, job.expectedLength)
        scheduledJob.addSegment(segment)
        self.scheduledJobs[job.id] = scheduledJob
        self.currentSchedule.addSegment(segment)

    def evaluateSchedule(self):
        self.currentSchedule.dump()
        sp = SchedulePerformance.SchedulePerformance(self.scheduledJobs)
        
        print("\n\n")
        return sp
