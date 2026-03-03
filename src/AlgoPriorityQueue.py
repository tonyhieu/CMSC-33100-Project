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
        nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        while (nextCore >= 0) and (self.currentSchedule.getExactEndTime(nextCore) < job.submissionTime):
            self.scheduleThreadFromHeapQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
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
            badCores = coreRestrictions.get(threadID, [])
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
                    priority = job.getTotalExpectedLength()
            thread.subThreads = AlgoBase.breakThreadIntoSubThreads(thread, 10)
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
        for threadID in range(job.nThreads):
            for subThreadID, subthread in enumerate(subThreads[threadID]):
                '''
                the time the next element will be released plus 
                the expected duration of all items with higher priority in the queue
                plus the expected duration of the thread
                '''
                expectedFinishTime = self.getQueueReleaseTime(job.submissionTime, threadCores[threadID][subThreadID]) \
                                        + self.getExpectedDurationUntilThread(threadCores[threadID][subThreadID], \
                                                                        threadPriorities[threadID][subThreadID],
                                                                        threadTieCounts[threadID][subThreadID]) \
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
        globalFloor = self.currentSchedule.previousSegmentAddTime
        if scheduleTime is None:
            threadStartTime = max(globalFloor,
                                  self.currentSchedule.getExactEndTime(coreID), 
                                  threadToSchedule.submissionTime)
        else:
            threadStartTime = max(scheduleTime,
                                  globalFloor,
                                  self.currentSchedule.getExactEndTime(coreID), 
                                  threadToSchedule.submissionTime)
        threadEndTime = threadStartTime + threadToSchedule.actualLength
        if (not np.isfinite(threadStartTime)):
            raise ValueError("Assigning an infinite start time!")
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
        removedFromQueue = []
        while len(self.jobQueue[coreID]) > 0:
            queueElement = heapq.heappop(self.jobQueue[coreID])
            removedFromQueue.append(queueElement)
            if (threadPriority == queueElement[0]) and (threadTieCount == queueElement[1]):
                break
            expectedQueueDuration += queueElement[2].expectedLength
        for queueElement in removedFromQueue:
            heapq.heappush(self.jobQueue[coreID], queueElement)
        return expectedQueueDuration
                

    def evaluateSchedule(self, verbose=False):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        print("evaluating Scchedule")
        nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        while (nextCore >= -1):
            if nextCore >= 0:
                self.scheduleThreadFromHeapQueue(nextCore)
            else:
                self.currentSchedule.dumpLasts()
                self.dumpDeadlock()
                raise ValueError("Deadlock!")
                self.resolveDeadlock()
            nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty

        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType, self.currentSchedule,
                                 verbose=verbose)
        if verbose:
            print("\n\n")
        return sp

    def resolveDeadlock(self):
        '''
        When all cores are blocked, we must gracefully preempt them. We pop the blocked 
        segments, let the next pending threads run, and requeue the blocked threads with
        a lower priority so they don't immediately lock the core again.
        '''
        globalTime = self.currentSchedule.previousSegmentAddTime
        recovered = False
        coresToRecover = []
        
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
                    addBack = True
                    nextInQueue = heapq.heappop(self.jobQueue[coreID])
                    if (nextInQueue[2].threadID == removedSegment.threadID) and \
                               (nextInQueue[2].jobID == removedSegment.jobID):
                        nextInQueue[2].submissionTime = globalTime
                        removedSubThreads.append(nextInQueue)
                        addBack = False
                    else:
                        sameThread = False
                else:
                    sameThread = False
                if addBack:
                    heapq.heappush(self.jobQueue[coreID], nextInQueue)

            if len(self.jobQueue[coreID]) > 0:
                coresToRecover.append(coreID)

            originalSubThread = \
                self.scheduledJobs[removedSegment.jobID].threads[removedSegment.threadID].subThreads[removedSegment.subThreadID]
            originalSubThread.submissionTime = globalTime
            
            heapq.heappush(self.jobQueue[coreID], (originalSubThread.priority + 10.0, originalSubThread.tieBreaker, originalSubThread))
            self.queueExpectedDuration[coreID] += originalSubThread.expectedLength
            for priority, tieCount, removedSubThread in removedSubThreads:
                heapq.heappush(self.jobQueue[coreID], (priority + 10.0, tieCount, removedSubThread))
            
            recovered = True

        def recoveryStart(coreID):
            if len(self.jobQueue[coreID]) == 0:
                return np.inf
            priority, tie, nextThread = heapq.heappop(self.jobQueue[coreID])
            ret =  max(globalTime,
                       self.currentSchedule.getExactEndTime(coreID),
                       nextThread.submissionTime)
            heapq.heappush(self.jobQueue[coreID], (priority, tie, nextThread))
            return ret

        for coreID in sorted(coresToRecover, key=recoveryStart):
            self.scheduleThreadFromHeapQueue(coreID, scheduleTime=globalTime)
            
        if not recovered:
            raise ValueError("ALL SEGMENTS ARE BLOCKED AND COULD NOT BE RESOLVED")

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
            return -2
        if earliestCore < 0:
            return -1
        return earliestCore

    def dumpQueue(self):
        for coreID, queue in enumerate(self.jobQueue):
            print(f"On Core {coreID:5}")
            for i in range(5):
                _, _, subThread = heapq.heappop(self.jobQueue[coreID])
                subThread.dump()
            if len(self.jobQueue[coreID]) > 0:
                print("....and more...")

    def dumpDeadlock(self):
        for coreSchedule in self.currentSchedule.schedule:
            lastSegment = coreSchedule[-1]
            if lastSegment.waiting:
                print("\n\n\n\n")
                lastSegment.dump()
                print("Subthreads of the same job in queue:")
                for coreID, queue in enumerate(self.jobQueue):
                    removedFromQueue = []
                    count = 0
                    while len(self.jobQueue[coreID]) > 0:
                        count += 1
                        queueElement = heapq.heappop(self.jobQueue[coreID])
                        if queueElement[2].jobID == lastSegment.jobID:
                            print(f"{count}th On Core: {coreID} with priority: {queueElement[2].priority:8.3f} and {queueElement[0]:8.3f}")
                            queueElement[2].dump()
                            print()
                        removedFromQueue.append(queueElement)
                    for queueElement in removedFromQueue:
                        heapq.heappush(self.jobQueue[coreID], queueElement)

        len7 = len(self.jobQueue[7])
        while len(self.jobQueue[7]) > len7 - 10:
            _, _, subThread = heapq.heappop(self.jobQueue[7])
            print(f"{len7 - len(self.jobQueue[7])}th On Core: {7} with priority: {subThread.priority:8.3f}")
            subThread.dump()
            print()

