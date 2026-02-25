from src import AlgoBase
from src import SchedulePerformance
from src import ScheduledJob
from src import Segment
from queue import Queue


class AlgoFIFO(AlgoBase.AlgoBase):

    def __init__(self):
        super().__init__("FIFO")
        self.jobQueue = Queue()
        self.queueExpectedDuration = 0.0

    def handleJobSubmission(self, job):

        '''
        we place ourselves in the moment of the scheduler right at this current
        jobs submission time 

        first we must add all jobs from queue to the schedule after converting them to segments, until 
        the most recently scheduled segment is currently running (its finish 
        time is after the current jobs submission time)

        the scheduler could be in a waiting state- at the time of submission, all
        segments have finished and nothing in the queue. This is handled by looking at 
        the jobQueue size
        '''
        
        scheduleEndTime = self.currentSchedule.getExactEndTime()
        while ((self.jobQueue.qsize() > 0) and  (scheduleEndTime < job.submissionTime)):
            self.scheduleJobFromQueue()
            scheduleEndTime = self.currentSchedule.getExactEndTime()
        
        '''
        we now add the current job to the queue and estimate its finish time
        '''

        '''this checks if a job is currently running at the time of submission
        if so, we need the expected end time of only the last job, since
        we know the time previous jobs finished
        '''
        if job.submissionTime < self.currentSchedule.getLastJobsExpectedEndTime():
            expectedStartTime = self.currentSchedule.getLastJobsExpectedEndTime() \
                                    + self.queueExpectedDuration
        else:
            expectedStartTime = job.submissionTime + self.queueExpectedDuration

        expectedEndTime = expectedStartTime + job.expectedLength
        scheduledJob = ScheduledJob.ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(expectedEndTime)

        self.jobQueue.put(scheduledJob)
        self.queueExpectedDuration += job.expectedLength
        self.scheduledJobs[job.id] = scheduledJob

    def scheduleJobFromQueue(self):
        if self.jobQueue.qsize() == 0:
            raise ValueError("No Jobs in Queue to Schedule")

        jobToSchedule = self.jobQueue.get()
        self.queueExpectedDuration -= jobToSchedule.expectedLength
        jobStartTime = max(self.currentSchedule.getExactEndTime(), 
                           jobToSchedule.submissionTime)
        jobEndTime = jobStartTime + jobToSchedule.intervalLength

        segmentID = jobToSchedule.getNumberOfSegments()
        segment = Segment.Segment(segmentID, jobStartTime, jobEndTime, jobToSchedule.id, jobToSchedule.expectedLength)
        self.scheduledJobs[jobToSchedule.id].addSegment(segment)
        self.currentSchedule.addSegment(segment)    
        


    def evaluateSchedule(self):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        while self.jobQueue.qsize() > 0:
            self.scheduleJobFromQueue()


        self.currentSchedule.dump()
        sp = SchedulePerformance.SchedulePerformance(self.scheduledJobs)
        
        print("\n\n")
        return sp
