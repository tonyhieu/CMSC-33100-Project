from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment
from .Job import Job
import heapq
import itertools
from enum import Enum
import numpy as np

class PriorityType(Enum):
    expectedLength = "expectedLength"

tieBreakingCounter = itertools.count()
class AlgoPriorityQueue(AlgoBase):

    def __init__(self, nCores, priorityType):
        super().__init__("PriorityQueue", nCores)
        self.nCores = nCores
        self.priorityType = priorityType
        self.jobQueue = [[] for _ in range(self.nCores)] #initialize list which heapq uses
        self.queueExpectedDuration = [0.0 for _ in range(self.nCores)]

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
        we now add the current job's threads into the various priority queues, based on the 
        total running time accumulated in each core's queues
        '''
        threadPriorities = np.zeros(job.nThreads)
        threadTieCounts = np.zeros(job.nThreads, dtype = int)
        threadCores = np.zeros(job.nThreads, dtype = int)
        for threadID, thread in enumerate(job.threads):
            #check core with earliest expected start time
            earliestCore = 0
            earliestQueueTime = self.getQueueFinishTime(job.submissionTime, 0)
            for coreID in range(1, self.nCores):
                coreEarliestQueueTime = self.getQueueFinishTime(job.submissionTime, coreID)
                if coreEarliestQueueTime < earliestQueueTime:
                    earliestQueueTime = coreEarliestQueueTime
                    earliestCore = coreID

            if self.priorityType == PriorityType.expectedLength:
                priority = thread.expectedLength

            tieCount = next(tieBreakingCounter)
            heapq.heappush(self.jobQueue[earliestCore], (priority, tieCount, thread))
            self.queueExpectedDuration[earliestCore] += thread.expectedLength
            threadPriorities[threadID] = priority
            threadTieCounts[threadID] = tieCount
            threadCores[threadID] = coreID

        """
        now that we added all of the threads to a queue, we need to calculate the 
        expected end time for the job by finding maximum thread end time
        """
        maximumExpectedFinishTime = 0.0
        for theadID, thread in enumerate(job.threads):
            '''
            the time the next element will be released plus 
            the expected duration of all items with higher priority in the queue
            plus the expected duration of the thread
            '''
            expectedFinishTime = self.getQueueReleaseTime(job.submissionTime, threadCores[theadID]) \
                                    + self.getExpectedDurationUntilThread(threadCores[theadID], 
                                                                     threadPriorities[theadID],
                                                                     threadTieCounts[theadID]) \
                                    + thread.expectedLength
            maximumExpectedFinishTime = max(maximumExpectedFinishTime, expectedFinishTime)                   


        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(maximumExpectedFinishTime)
        self.scheduledJobs[job.id] = scheduledJob

    def scheduleThreadFromHeapQueue(self, coreID):
        if len(self.jobQueue[coreID]) == 0:
            raise ValueError("No Jobs in Queue to Schedule")

        priority, tieCount, threadToSchedule = heapq.heappop(self.jobQueue[coreID])
        self.queueExpectedDuration[coreID] -= threadToSchedule.expectedLength
        threadStartTime = max(self.currentSchedule.getExactEndTime(coreID), 
                           threadToSchedule.submissionTime)
        threadEndTime = threadStartTime + threadToSchedule.actualLength

        segmentID = self.scheduledJobs[threadToSchedule.jobID].getNumberOfScheduledSegments()
        segment = Segment(segmentID, 
                          threadToSchedule.jobID, 
                          coreID, 
                          threadToSchedule.threadID, 
                          threadStartTime, 
                          threadEndTime, 
                          threadToSchedule.expectedLength)
        self.scheduledJobs[threadToSchedule.jobID].addSegment(segment)
        self.currentSchedule.addSegment(segment)

    def getQueueFinishTime(self, submissionTime, coreID):
        '''
        the expected finish time for the queue is the time the next 
        element will be released
        plus the expected length of the queue
        '''
        return self.getQueueReleaseTime(submissionTime, coreID) \
                + self.queueExpectedDuration[coreID]
    
    def getQueueReleaseTime(self, submissionTime, coreID):
        '''
        the expected release time for the queue on a specific core is either the submission time, or the 
        expected finish time of the last job, whichever is latest
        '''
        return max(submissionTime, self.currentSchedule.getLastJobsExpectedEndTime(coreID))

    def getExpectedDurationUntilThread(self, coreID, threadPriority, threadTieCount):
        expectedQueueDuration = 0.0
        """
        go through queue, and add up the expected wait time of everything 
        that will be released before it (lower priority)
        """
        for priority, tieCount, thread in self.jobQueue[coreID]:
            if threadPriority == priority:
                if tieCount < threadTieCount:
                    expectedQueueDuration += thread.expectedLength
            elif priority < threadPriority:
                expectedQueueDuration += thread.expectedLength

        return expectedQueueDuration
                

    def evaluateSchedule(self):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        for coreID in range(self.nCores):
            while len(self.jobQueue[coreID]) > 0:
                self.scheduleThreadFromHeapQueue(coreID)

        self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType)
        
        print("\n\n")
        return sp
