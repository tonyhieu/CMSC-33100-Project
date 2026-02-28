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

    def __init__(self, nCores, priorityType, globalSemaphoreList):
        super().__init__("PriorityQueue", nCores, globalSemaphoreList)
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
        nextCore = self.getCoreNextToBeScheduled() #negative means all queue are emepty
        nextCoreEndTime = self.currentSchedule.getExactEndTime(nextCore)
        while (nextCore >= 0) and (nextCoreEndTime < job.submissionTime):
            self.scheduleThreadFromHeapQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #negative means all queue are emepty
            nextCoreEndTime = self.currentSchedule.getExactEndTime(nextCore)
        '''
        we now add the current job's threads into the various priority queues, based on the 
        total running time accumulated in each core's queues
        '''
        subThreads = [[] for _ in range(job.nThreads)]
        threadPriorities = [[] for _ in range(job.nThreads)]
        threadTieCounts = [[] for _ in range(job.nThreads)]
        threadCores = [[] for _ in range(job.nThreads)]
        coreRestrictions = {}
        for threadID, thread in enumerate(job.threads):
            if threadID in coreRestrictions:
                badCores = coreRestrictions[threadID]
            else:
                badCores = []
            #check core with earliest expected start time
            earliestCore = -1
            earliestQueueTime = np.inf
            for coreID in range(self.nCores):
                if coreID in badCores:
                    continue
                coreEarliestQueueTime = self.getQueueFinishTime(job.submissionTime, coreID)
                if coreEarliestQueueTime < earliestQueueTime:
                    earliestQueueTime = coreEarliestQueueTime
                    earliestCore = coreID
            if earliestCore == -1:
                raise ValueError("This Should not be possible")
            #we will schedule this thread on core earliestCore, so put that in synchronized thread's core restrictions
            for syncedThread in job.synchronizedThreads[threadID]:
                if syncedThread in coreRestrictions:
                    coreRestrictions[syncedThread].append(earliestCore)
                else:
                    coreRestrictions[syncedThread] = [earliestCore]
            if self.priorityType == PriorityType.expectedLength:
                    #all subthreads get the same priority to ensure they run in order on the cores
                    priority = thread.expectedLength
            for subthread in (AlgoBase.breakThreadIntoSubThreads(thread)):
                tieCount = next(tieBreakingCounter)
                heapq.heappush(self.jobQueue[earliestCore], (priority, tieCount, subthread))
                subThreads[threadID].append(subthread)
                threadPriorities[threadID].append(priority)
                threadTieCounts[threadID].append(tieCount)
                threadCores[threadID].append(earliestCore)
            self.queueExpectedDuration[earliestCore] += thread.expectedLength
        """
        now that we added all of the threads to a queue, we need to calculate the 
        expected end time for the job by finding maximum thread end time
        """
        maximumExpectedFinishTime = 0.0
        for theadID in range(job.nThreads):
            for subThreadID, subthread in enumerate(subThreads[theadID]):
                '''
                the time the next element will be released plus 
                the expected duration of all items with higher priority in the queue
                plus the expected duration of the thread
                '''
                expectedFinishTime = self.getQueueReleaseTime(job.submissionTime, threadCores[theadID][subThreadID]) \
                                        + self.getExpectedDurationUntilThread(threadCores[theadID][subThreadID], 
                                                                        threadPriorities[theadID][subThreadID],
                                                                        threadTieCounts[theadID][subThreadID]) \
                                        + subthread.expectedLength
                maximumExpectedFinishTime = max(maximumExpectedFinishTime, expectedFinishTime)                   


        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(maximumExpectedFinishTime)
        self.scheduledJobs[job.id] = scheduledJob

    def scheduleThreadFromHeapQueue(self, coreID):
        if len(self.jobQueue[coreID]) == 0:
            raise ValueError("No Jobs in Queue to Schedule")
        if self.currentSchedule.isCoreBlocked(coreID) >= 0:
            return self.currentSchedule.isCoreBlocked(coreID)
        priority, tieCount, threadToSchedule = heapq.heappop(self.jobQueue[coreID])
        self.queueExpectedDuration[coreID] -= threadToSchedule.expectedLength
        threadStartTime = max(self.currentSchedule.getExactEndTime(coreID), 
                           threadToSchedule.submissionTime)
        threadEndTime = threadStartTime + threadToSchedule.actualLength
        if (not np.isfinite(threadStartTime)):
            raise ValueError("Assignening an infinite start time!")
        segmentID = self.scheduledJobs[threadToSchedule.jobID].getNumberOfScheduledSegments()
        segment = Segment(segmentID,
                          coreID, 
                          threadToSchedule, 
                          threadStartTime, 
                          threadEndTime)
        self.scheduledJobs[threadToSchedule.jobID].addSegment(segment)
        self.currentSchedule.addSegment(segment) 
        return -1

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
        if len(self.jobQueue[coreID]) == 0:
            return expectedQueueDuration
        priorityIndex = 0
        notFound = True
        while notFound:
            priority, tieCount, thread = self.jobQueue[coreID][priorityIndex]
            priorityIndex += 1
            if threadPriority == priority:
                if tieCount == threadTieCount:
                    notFound = False
                else:
                    expectedQueueDuration += thread.expectedLength
            else:
                expectedQueueDuration += thread.expectedLength
        return expectedQueueDuration
                

    def evaluateSchedule(self):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        nextCore = self.getCoreNextToBeScheduled() #negative means all queue are empty
        while (nextCore >= 0):
            self.scheduleThreadFromHeapQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #negative means all queue are empty

        self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType)
        
        print("\n\n")
        return sp

    def getCoreNextToBeScheduled(self):
        earlistStartTime = np.inf
        earliestCore = -1
        allQueuesEmpty = True
        for coreID in range(self.nCores):
            if len(self.jobQueue[coreID]) == 0:
                continue
            allQueuesEmpty = False
            if self.currentSchedule.isCoreBlocked(coreID) >= 0:
                continue
            priroty, tieBreeaker, nextThread = heapq.heappop(self.jobQueue[coreID])
            nextThreadStart = max(self.currentSchedule.getExactEndTime(coreID), 
                           nextThread.submissionTime)
            heapq.heappush(self.jobQueue[coreID], (priroty, tieBreeaker, nextThread))
            if (nextThreadStart < earlistStartTime):
                earlistStartTime = nextThreadStart
                earliestCore = coreID
        if allQueuesEmpty:
            return -1
        if earliestCore < 0:
            print("Dumping: ")
            self.currentSchedule.dumpLasts()
            self.dumpQueue()
            raise ValueError("ALL SEGMENTS ARE BLOCKED")
        return earliestCore

    def dumpQueue(self):
        for coreID, queue in enumerate(self.jobQueue):
            print(f"On Core {coreID:5}")
            for _, _, subThread in self.jobQueue[coreID]:
                subThread.dump()