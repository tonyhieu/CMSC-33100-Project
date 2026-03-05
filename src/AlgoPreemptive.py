from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .AlgoPreemptivePriorityQueue import PriorityType
from .Segment import Segment
from .Job import Job
import heapq
import itertools
from enum import Enum
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
floatPrecision = float(os.getenv("FLOAT_PRECISION"))

"""
This is equivalent to prioritty queue but it removes jobs that 
are waiting to be resumed later
"""

tieBreakingCounter = itertools.count()
class AlgoPreemptive(AlgoBase):

    def __init__(self, nCores, priorityType, globalSemaphoreList):
        raise ValueError("Duncan thinks this Algo is deprecated and shouldn't be used, use PPQ instead")
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
        schedulingTime = None
        while (nextCore >= 0) and (nextCoreEndTime < job.submissionTime):
            schedulingTime = self.getNextThreadStart(nextCore)
            self.scheduleThreadFromHeapQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #negative means all queue are emepty
            nextCoreEndTime = self.currentSchedule.getExactEndTime(nextCore)
        if not (schedulingTime is None):
            self.doPreemption(schedulingTime)
        if nextCore == -1:
            raise ValueError("NO Submission occurred")
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
            thread.subThreads = AlgoBase.breakThreadIntoSubThreads(thread)
            for subthread in (thread.subThreads):
                tieCount = next(tieBreakingCounter)
                subthread.priority = priority
                subthread.tieBreaker = tieCount
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

    def scheduleThreadFromHeapQueue(self, coreID, scheduleTime = None):
        if len(self.jobQueue[coreID]) == 0:
            raise ValueError("No Jobs in Queue to Schedule")
        if self.currentSchedule.isCoreBlocked(coreID) >= 0:
            return self.currentSchedule.isCoreBlocked(coreID)
        priority, tieCount, threadToSchedule = heapq.heappop(self.jobQueue[coreID])
        self.queueExpectedDuration[coreID] -= threadToSchedule.expectedLength
        if scheduleTime is None:
            threadStartTime = max(self.currentSchedule.getExactEndTime(coreID), 
                            threadToSchedule.submissionTime)
        else:
            threadStartTime = max(scheduleTime, self.currentSchedule.getExactEndTime(coreID))
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
        if self.queueExpectedDuration[coreID] < -floatPrecision:
            raise ValueError(f"queueExpectedDuration on core {coreID} is negative!")
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
                

    def evaluateSchedule(self, verbose=False):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        while (nextCore >= -1):
            if nextCore >= 0:
                self.scheduleThreadFromHeapQueue(nextCore)
            else:
                recoveryTime = self.getBlockedRecoveryTime()
                if recoveryTime is None:
                    raise ValueError("ALL SEGMENTS ARE BLOCKED")
                self.doPreemption(recoveryTime)
            nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
            

        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType, self.currentSchedule,
                                 verbose=verbose)
        if verbose:
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
            nextThreadStart = self.getNextThreadStart(coreID)
            if (nextThreadStart < earlistStartTime):
                earlistStartTime = nextThreadStart
                earliestCore = coreID
        if allQueuesEmpty:
            return -2
        return earliestCore

    def getBlockedRecoveryTime(self):
        foundBlocked = False
        for coreID in range(self.nCores):
            if self.currentSchedule.isCoreBlocked(coreID) >= 0:
                foundBlocked = True
        if not foundBlocked:
            return None
        return self.currentSchedule.previousSegmentAddTime

    def doPreemption(self, globalTime):
        '''
        Find cores that are waiting, stop those segments, 
        and schedule the next thread on the queue in its place
        '''
        if globalTime is None:
            raise ValueError("Trying to do preemption when global Time is None")
            
        for coreID in range(self.nCores):
            if self.currentSchedule.isCoreBlocked(coreID) < 0:
                continue

            removedSegment = self.currentSchedule.removeLastScheduledSegment(coreID)
            removedSegment.startTime = -1.0
            removedSegment.endTime = -1.0
            removedSegment.expectedDuration = 0.0

            removedSubThreads = []
            sameThread = True
            while sameThread:
                if len(self.jobQueue[coreID]) > 0:
                    nextInQueue = self.jobQueue[coreID][0]
                    if (nextInQueue[2].threadID == removedSegment.threadID) and \
                                (nextInQueue[2].jobID == removedSegment.jobID):
                        nextInQueue[2].submissionTime = globalTime
                        nextInQueue = heapq.heappop(self.jobQueue[coreID])
                        removedSubThreads.append(nextInQueue)
                    else:
                        sameThread = False
                else:
                    sameThread = False

            if len(self.jobQueue[coreID]) > 0:
                self.scheduleThreadFromHeapQueue(coreID, scheduleTime = globalTime)

            originalSubThread = \
                self.scheduledJobs[removedSegment.jobID].threads[removedSegment.threadID].subThreads[removedSegment.subThreadID]
            originalSubThread.submissionTime = globalTime
            
            heapq.heappush(self.jobQueue[coreID], (originalSubThread.priority + 10.0, originalSubThread.tieBreaker, originalSubThread))
            self.queueExpectedDuration[coreID] += originalSubThread.expectedLength
            for priority, tieCount, removedSubThread in removedSubThreads:
                heapq.heappush(self.jobQueue[coreID], (priority + 10.0, tieCount, removedSubThread))
                    


    def dumpQueue(self):
        for coreID, queue in enumerate(self.jobQueue):
            print(f"On Core {coreID:5}")
            for _, _, subThread in self.jobQueue[coreID]:
                subThread.dump()

    def getNextThreadStart(self, coreID):
        if len(self.jobQueue[coreID]) > 0:
            priroty, tieBreeaker, nextThread = heapq.heappop(self.jobQueue[coreID])
            start = max(self.currentSchedule.getExactEndTime(coreID), nextThread.submissionTime)
            heapq.heappush(self.jobQueue[coreID], (priroty, tieBreeaker, nextThread))
            return start
                           