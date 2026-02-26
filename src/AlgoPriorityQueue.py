from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment
from .Job import Job
import heapq
import itertools
from enum import Enum

class PriorityType(Enum):
    expectedLength = "expectedLength"

tieBreakingCounter = itertools.count()
class AlgoPriorityQueue(AlgoBase):

    def __init__(self, nCores, priorityType):
        super().__init__("PriorityQueue", nCores)
        self.nCores = nCores
        self.priorityType = priorityType
        self.jobQueue = [[] for _ in range(self.nCores)] #initialize list which heapq uses

    def handleJobSubmission(self, job: Job):

        '''
        we place ourselves in the moment of the scheduler right at this current
        jobs submission time 

        first we must add all threads from heap queue to the schedule, until 
        the most recently scheduled threads is currently running (its finish 
        time is after the current jobs submission time)

        the scheduler could be in a waiting state- at the time of submission, all
        segments have finished and nothing in the queue. This is handled by looking at 
        the jobQueue length
        '''
        
        for coreID in range(self.nCores):
            coreEndTime = self.currentSchedule.getExactEndTime(coreID)
            while ((len(self.jobQueue[coreID]) > 0) and  (coreEndTime < job.submissionTime)):
                self.scheduleThreadFromHeapQueue(coreID)
                coreEndTime = self.currentSchedule.getExactEndTime(coreID)
        
        '''
        we now "stop" the current segment, split the part that has not finished into
        a smaller segment, then add both the running segment and the submitted job(conveerted to segment)
        into the heap queue
        '''

        #first check if job is currently running, trim and add not run part in queue
        if job.submissionTime < self.currentSchedule.getExactEndTime():
            runningSegment = self.currentSchedule.trimRunningSegment(job.submissionTime)
            runningTime = job.submissionTime - runningSegment.startTime#time the segment got to run
            #add the running segment as another job to the heap queue
            runningJob = ScheduledJob(self.scheduledJobs[runningSegment.jobID])

            if (runningTime > runningJob.expectedLength):
                expectedRemainingTime = 0.0
            else:
                expectedRemainingTime = runningJob.expectedLength - runningTime
            heapq.heappush(self.jobQueue, (expectedRemainingTime, next(tieBreakingCounter), runningJob))

        submittedJob = ScheduledJob(job)
        submittedJobTieCount = next(tieBreakingCounter)
        heapq.heappush(self.jobQueue, (submittedJob.expectedLength, submittedJobTieCount, submittedJob))
        expectedFinishTime = job.submissionTime + self.getExpectedDurationUntilJob(submittedJob.id, submittedJob.expectedLength, submittedJobTieCount) + submittedJob.expectedLength
        submittedJob.setExpectedFinishTime(expectedFinishTime)
        self.scheduledJobs[job.id] = submittedJob

    def scheduleThreadFromHeapQueue(self, coreID):
        if len(self.jobQueue) == 0:
            raise ValueError("No Jobs in Queue to Schedule")

        priority, tieCount, jobToSchedule = heapq.heappop(self.jobQueue)
        jobStartTime = max(self.currentSchedule.getExactEndTime(), 
                           jobToSchedule.submissionTime)
        jobEndTime = jobStartTime + jobToSchedule.intervalLength

        segmentID = self.scheduledJobs[jobToSchedule.id].getNumberOfSegments()
        segment = Segment(segmentID, jobStartTime, jobEndTime, jobToSchedule.id, jobToSchedule.expectedLength)
        self.scheduledJobs[jobToSchedule.id].addSegment(segment)
        self.currentSchedule.addSegment(segment)

    def getExpectedDurationUntilJob(self, jobID, jobPriority, jobTieCount):

        numJobsWithID = 0
        expectedQueueDuration = 0.0
        for priority, tieCount, job in self.jobQueue:         
            print(priority)
            print(jobPriority)
            if jobID == job.id:
                numJobsWithID += 1
                if numJobsWithID > 1:
                    raise ValueError("Multiple Jobs with same ID in heap Queue")
            elif jobPriority == priority:
                if tieCount < jobTieCount:
                    expectedQueueDuration += job.expectedLength
            elif priority < jobPriority:
                expectedQueueDuration += job.expectedLength

        return expectedQueueDuration
                

        


    def evaluateSchedule(self):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        while len(self.jobQueue) > 0:
            self.scheduleJobFromHeapQueue()

        self.currentSchedule.dump()
        sp = SchedulePerformance.SchedulePerformance(self.scheduledJobs)
        
        print("\n\n")
        return sp
