from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment
from collections import deque
import numpy as np
import sys

class AlgoFIFO(AlgoBase):

    def __init__(self, nCores, globalSemaphoreList):
        super().__init__("FIFO", nCores, globalSemaphoreList)
        #we create a seperate queue for each core available to schedule threads on
        self.jobQueue = [deque([]) for _ in range(self.nCores)]
        self.queueExpectedDuration = [0.0 for _ in range(self.nCores)]

    def handleJobSubmission(self, job):

        '''
        we place ourselves in the moment of the scheduler right at this current
        jobs submission time 

        first we must add all jobs from queue to the schedule after converting them to threads, until 
        the most recently scheduled threads is currently running (its finish 
        time is after the current jobs submission time)

        the scheduler could be in a waiting state- at the time of submission, all
        threads have finished and nothing in the queue. This is handled by looking at 
        the jobQueue size
        '''
        
        nextCore = self.getCoreNextToBeScheduled() #negative means all queue are emepty
        nextCoreEndTime = self.currentSchedule.getExactEndTime(nextCore)
        while (nextCore >= 0) and (nextCoreEndTime < job.submissionTime):
            self.scheduleThreadFromQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #negative means all queue are emepty
            nextCoreEndTime = self.currentSchedule.getExactEndTime(nextCore)
        
        '''
        we now add the current jobs threads to the queue and estimate the finish time of the last thread
        '''
        coreRestrictions = {}
        threadExpectedEndTimes = np.zeros(job.nThreads)
        for threadID, thread in enumerate(job.threads):
            if threadID in coreRestrictions:
                badCores = coreRestrictions[threadID]
            else:
                badCores = []
            #check core with earliest expected start time
            earliestCore = -1
            earliestExpectedStartTime = np.inf
            for coreID in range(self.nCores):
                if coreID in badCores:
                    continue
                coreExpectedStartTime = self.getEarliestExpectedStartTime(job.submissionTime, coreID)
                if coreExpectedStartTime < earliestExpectedStartTime:
                    earliestExpectedStartTime = coreExpectedStartTime
                    earliestCore = coreID
            if earliestCore == -1:
                raise ValueError("This Should not be possible")
            #we will schedule this thread on core earliestCore, so put that in synchronized thread's core restrictions
            for syncedThread in job.synchronizedThreads[threadID]:
                if syncedThread in coreRestrictions:
                    coreRestrictions[syncedThread].append(earliestCore)
                else:
                    coreRestrictions[syncedThread] = [earliestCore]

            thread.subThreads = AlgoBase.breakThreadIntoSubThreads(thread)
            for subthread in (thread.subThreads):
                
                self.jobQueue[earliestCore].append(subthread)
            self.queueExpectedDuration[earliestCore] += thread.expectedLength
            threadExpectedEndTimes[threadID] = earliestExpectedStartTime + thread.expectedLength

        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(np.max(threadExpectedEndTimes))
        self.scheduledJobs[job.id] = scheduledJob
        

    def scheduleThreadFromQueue(self, coreID):
        if len(self.jobQueue[coreID]) == 0:
            raise ValueError("No Jobs in Queue to Schedule")
        if self.currentSchedule.isCoreBlocked(coreID) >= 0:
            return self.currentSchedule.isCoreBlocked(coreID)
        threadToSchedule = self.jobQueue[coreID].popleft()
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

    def getEarliestExpectedStartTime(self, submissionTime, coreID):
        '''
        the expected start time on this core is either the submission time, or the 
        expected finish time of the last job, whichever is latest
        plus the expected length of the queue
        '''
        return max(submissionTime, self.currentSchedule.getLastJobsExpectedEndTime(coreID)) \
                + self.queueExpectedDuration[coreID]

        


    def evaluateSchedule(self, verbose=False):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        nextCore = self.getCoreNextToBeScheduled() #negative means all queue are empty
        while (nextCore >= 0):
            self.scheduleThreadFromQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #negative means all queue are empty


        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType, self.currentSchedule,
                                 verbose=verbose)
        if verbose:
            print("\n\n")
        
        print("\n\n")
        return sp

    def dumpQueue(self):
        for coreID, queue in enumerate(self.jobQueue):
            print(f"On Core {coreID:5}")
            while len(self.jobQueue[coreID]) > 0:
                self.jobQueue[coreID].popleft().dump()

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
            nextThread = self.jobQueue[coreID][0]
            nextThreadStart = max(self.currentSchedule.getExactEndTime(coreID), 
                           nextThread.submissionTime)
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
            raise ValueError("All Queues Are Blocked")
        return earliestCore



