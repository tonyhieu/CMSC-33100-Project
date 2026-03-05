from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment
from .Semaphore import SemOperation
from .Job import Job
import heapq
import itertools
from enum import Enum
import numpy as np
import sys
import bisect

class PriorityType(Enum):
    expectedLength = "expectedLength"

tieBreakingCounter = itertools.count()
class AlgoPreemptivePriorityQueue(AlgoBase):

    def __init__(self, nCores, priorityType, globalSemaphoreList):
        super().__init__("PriorityQueue", nCores, globalSemaphoreList)
        self.nCores = nCores
        self.priorityType = priorityType
        self.jobQueue = [[] for _ in range(self.nCores)] #initialize list which heapq uses
        self.queueExpectedDuration = [0.0 for _ in range(self.nCores)]
        self.semaphoreMapping = {i : [] for i in range(len(globalSemaphoreList))}
        self.queuePriorities = [[] for _ in range(self.nCores)]
        self.queueExpectedDurationArrays = [np.array([], dtype = float) for _ in range(self.nCores)]

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
        self.resolveDeadlock(False)
        nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        while (nextCore >= 0) and (self.currentSchedule.getExactEndTime(nextCore) < job.submissionTime):
            self.scheduleThreadFromHeapQueue(nextCore)
            nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        '''
        we now add the current job's threads into the various priority queues, based on the 
        total running time accumulated in each core's queues
        '''
        threadTieCounts = np.zeros(job.nThreads, dtype = int)
        threadCores = np.zeros(job.nThreads, dtype = int)
        coreRestrictions = {}


        if self.priorityType == PriorityType.expectedLength:
            #all subthreads get the same priority to ensure they run in order on the cores
            priority = job.getTotalExpectedLength()

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
            thread.subThreads = AlgoBase.breakThreadIntoSubThreads(thread, 10)
            tieCount = next(tieBreakingCounter)
            for subthread in (thread.subThreads):
                subthread.priority = priority
                subthread.tieBreaker = tieCount
                if subthread.start[2] == SemOperation.Post:
                    self.semaphoreMapping[subthread.start[0]].append((earliestCore))
            heapq.heappush(self.jobQueue[earliestCore], (priority, tieCount, thread, 0))#4th argument is the subthread the queue will release next
            threadTieCounts[threadID] = tieCount
            threadCores[threadID] = earliestCore
            self.queueExpectedDuration[earliestCore] += thread.expectedLength
            # idx = bisect.bisect_left(self.queuePriorities[earliestCore], (priority, tieCount))
            # self.queuePriorities[earliestCore].insert(idx, (priority, tieCount))
            # self.queueExpectedDurationArrays[earliestCore] = np.insert(self.queueExpectedDurationArrays[earliestCore], idx, thread.expectedLength)


        """
        now that we added all of the threads to a queue, we need to calculate the 
        expected end time for the job by finding maximum thread end time
        """
        maximumExpectedFinishTime = 0.0
        for threadID in range(job.nThreads):
            '''
            the time the next element will be released plus 
            the expected duration of all items with higher priority in the queue
            plus the expected duration of the thread
            '''
            expectedFinishTime = self.getQueueReleaseTime(job.submissionTime, threadCores[threadID]) \
                                    + self.getExpectedDurationUntilThread(threadCores[threadID], \
                                                                    priority,
                                                                    threadTieCounts[threadID]) \
                                    + job.threads[threadID].expectedLength
            maximumExpectedFinishTime = max(maximumExpectedFinishTime, expectedFinishTime)
        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(maximumExpectedFinishTime)
        self.scheduledJobs[job.id] = scheduledJob

    def scheduleThreadFromHeapQueue(self, coreID, scheduleTime = None):
        if len(self.jobQueue[coreID]) == 0:
            raise ValueError("No Jobs in Queue to Schedule")
        if self.currentSchedule.isCoreBlocked(coreID) >= 0:
            return self.currentSchedule.isCoreBlocked(coreID)
        priority, tieCount, threadToSchedule, subThreadID = heapq.heappop(self.jobQueue[coreID])
        if subThreadID >= len(threadToSchedule.subThreads):
            raise ValueError("Trying to access a subthread that does not exist")
        subThreadToSchedule = threadToSchedule.subThreads[subThreadID]
        self.queueExpectedDuration[coreID] -= subThreadToSchedule.expectedLength
        # self.queueExpectedDurationArrays[coreID][0] -= subThreadToSchedule.expectedLength
        globalFloor = self.currentSchedule.previousSegmentAddTime
        if scheduleTime is None:
            threadStartTime = max(globalFloor,
                                  self.currentSchedule.getExactEndTime(coreID), 
                                  subThreadToSchedule.submissionTime)
        else:
            threadStartTime = max(scheduleTime,
                                  globalFloor,
                                  self.currentSchedule.getExactEndTime(coreID), 
                                  subThreadToSchedule.submissionTime)
        threadEndTime = threadStartTime + subThreadToSchedule.actualLength
        if (not np.isfinite(threadStartTime)):
            raise ValueError("Assigning an infinite start time!")
        segmentID = self.scheduledJobs[subThreadToSchedule.jobID].getNumberOfScheduledSegments()
        segment = Segment(segmentID,
                          coreID, 
                          subThreadToSchedule, 
                          threadStartTime, 
                          threadEndTime)
        self.scheduledJobs[subThreadToSchedule.jobID].addSegment(segment)
        addResult = self.currentSchedule.addSegment(segment) #True is segment has to wait, change prioiry of all threads with a post
        if addResult:
            for mappedCore in self.semaphoreMapping[segment.start[0]]:
                self.makePostsToWaitingSegmentsMoreUrgent(mappedCore, segment.jobID)

        if subThreadID < len(threadToSchedule.subThreads) - 1:
            heapq.heappush(self.jobQueue[coreID], (priority, tieCount, threadToSchedule, subThreadID + 1))
        # else:
        #     self.queueExpectedDurationArrays[coreID] = self.queueExpectedDurationArrays[coreID][1:]
        #     self.queuePriorities[coreID].pop(0)

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

    def getExpectedDurationUntilThread1(self, coreID, threadPriority, threadTieCount):
        idx = bisect.bisect_left(self.queuePriorities[coreID], (threadPriority, threadTieCount))
        return np.sum(self.queueExpectedDurationArrays[coreID][:idx])

    def getExpectedDurationUntilThread2(self, coreID, threadPriority, threadTieCount):
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
            loopSTID = queueElement[3]
            while loopSTID < len(queueElement[2].subThreads):
                expectedQueueDuration += queueElement[2].subThreads[loopSTID].expectedLength
                loopSTID+=1
        for queueElement in removedFromQueue:
            heapq.heappush(self.jobQueue[coreID], queueElement)
        return expectedQueueDuration

    def getExpectedDurationUntilThread(self, coreID, threadPriority, threadTieCount):
        epectedDuration = self.getExpectedDurationUntilThread2(coreID, threadPriority, threadTieCount)
        return epectedDuration
                

    def evaluateSchedule(self, verbose=False):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        print("evaluating Scchedule")
        nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty
        initialQueueLength = len(self.jobQueue[nextCore])
        while (nextCore >= -1):
            AlgoPreemptivePriorityQueue._print_status(initialQueueLength - len(self.jobQueue[nextCore]), initialQueueLength)
            if nextCore >= 0:
                self.scheduleThreadFromHeapQueue(nextCore)
            else:
                self.resolveDeadlock(True)
            nextCore = self.getCoreNextToBeScheduled() #>=0 runnable, -1 blocked, -2 empty

        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType, self.currentSchedule,
                                 verbose=verbose)
        if verbose:
            print("\n\n")
        return sp

    def resolveDeadlock(self, raiseDeadlockError):
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
            self.freeCore(coreID)
            if len(self.jobQueue[coreID]) > 0:
                coresToRecover.append(coreID)
            recovered = True

        def recoveryStart(coreID):
            if len(self.jobQueue[coreID]) == 0:
                return np.inf
            priority, tie, nextThread, subThreeadID = self.jobQueue[coreID][0]
            ret =  max(globalTime,
                       self.currentSchedule.getExactEndTime(coreID),
                       nextThread.submissionTime)
            return ret

        for coreID in sorted(coresToRecover, key=recoveryStart):
            self.scheduleThreadFromHeapQueue(coreID, scheduleTime=globalTime)
            
        if not recovered and raiseDeadlockError:
            self.currentSchedule.dumpLasts()
            self.dumpDeadlock()
            raise ValueError("Deadlock!")

    def freeCore(self, coreID):
        '''
        remove the waiting segment on the core and place bacck in the queue
        '''
        removedSegment = self.currentSchedule.removeLastScheduledSegment(coreID)
        removedSegment.startTime = -1.0
        removedSegment.endTime = -1.0
        removedSegment.expectedDuration = 0.0

        if removedSegment.subThreadID \
            == len(self.scheduledJobs[removedSegment.jobID].threads[removedSegment.threadID].subThreads) - 1:
            '''
            This removed segment is the last subthread of its thread, meaning the original thread 
            was removed from the queue, so we need to add the thread back to the queue
            '''
            originalThread = self.scheduledJobs[removedSegment.jobID].threads[removedSegment.threadID]
            priority = originalThread.subThreads[removedSegment.subThreadID].priority
            tieBreaker = originalThread.subThreads[removedSegment.subThreadID].tieBreaker
            heapq.heappush(self.jobQueue[coreID], (priority, tieBreaker,
                                                   originalThread,
                                                   removedSegment.subThreadID))
            # idx = bisect.bisect_left(self.queuePriorities[coreID], (priority, tieBreaker))
            # self.queuePriorities[coreID].insert(idx, (priority, tieBreaker))
            # self.queueExpectedDurationArrays[coreID] = np.insert(self.queueExpectedDurationArrays[coreID], idx, removedSegment.expectedDuration)

        else:
            '''
            we need to search the queue to find the original thread
            '''
            removedThreads = []
            while len(self.jobQueue[coreID]) > 0:
                nextInQueue = heapq.heappop(self.jobQueue[coreID])
                if (nextInQueue[2].threadID == removedSegment.threadID) and \
                            (nextInQueue[2].jobID == removedSegment.jobID):
                    removedThreads.append((nextInQueue[0], nextInQueue[1], nextInQueue[2], removedSegment.subThreadID))
                    break
                else:
                    removedThreads.append(nextInQueue)
            for queueElement in removedThreads:
                heapq.heappush(self.jobQueue[coreID], queueElement)
            # idx = len(removedThreads) - 1
            # self.queueExpectedDurationArrays[coreID][idx] += removedSegment.expectedDuration


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
            priroty, tieBreaker, nextThread, subThreadID = self.jobQueue[coreID][0]
            nextThreadStart = max(self.currentSchedule.getExactEndTime(coreID), 
                           nextThread.submissionTime)
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
                _, _, thread, subThreadID = heapq.heappop(self.jobQueue[coreID])
                while subThreadID < len(thread.subThreads):
                    thread.subThreads[subThreadID].dump()
                    subThreadID += 1
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
                            subThreadID = queueElement[3]
                            while subThreadID < len(queueElement[2].subThreads):
                                print(f"{count}th On Core: {coreID} with priority: {queueElement[2].subThreads[subThreadID].priority:8.3f} and {queueElement[0]:8.3f}")
                                queueElement[2].subThreads[subThreadID].dump()
                                subThreadID+=1
                            print()
                        removedFromQueue.append(queueElement)
                    for queueElement in removedFromQueue:
                        heapq.heappush(self.jobQueue[coreID], queueElement)


    def makePostsToWaitingSegmentsMoreUrgent(self, coreID, jobID):
        queueSizeBefore = len(self.jobQueue[coreID])
        removedFromQueue = []
        durationChanges = []
        while len(self.jobQueue[coreID]) > 0:
            queueElement = heapq.heappop(self.jobQueue[coreID])
            priorityChange = False
            if (queueElement[2].jobID == jobID):
                loopSTID = queueElement[3]
                while loopSTID < len(queueElement[2].subThreads):
                    if queueElement[2].subThreads[loopSTID].start[2] == SemOperation.Post:
                        priorityChange = True
                        break
                    loopSTID += 1
            if priorityChange:
                loopSTID = queueElement[3]
                while loopSTID < len(queueElement[2].subThreads):
                    queueElement[2].subThreads[loopSTID].priority = -1.0
                    loopSTID+=1
                removedFromQueue.append((-1.0, queueElement[1], queueElement[2], queueElement[3]))
                # listIdx = len(removedFromQueue) - 1
                # durationChanges.append((listIdx, queueElement[1], self.queueExpectedDurationArrays[coreID][listIdx]))
            else:
                removedFromQueue.append(queueElement)


        for queueElement in removedFromQueue:
            heapq.heappush(self.jobQueue[coreID], queueElement)

        count = 0
        for durationChange in durationChanges:
            self.queuePriorities[coreID].pop(durationChange[0] - count)
            self.queueExpectedDurationArrays[coreID] = np.delete(self.queueExpectedDurationArrays[coreID], durationChange[0] - count)
            count += 1
        # for durationChange in durationChanges:
        #     newPriorities = (-1.0, durationChange[1])
        #     idx = bisect.bisect_left(self.queuePriorities[coreID], newPriorities)
        #     self.queuePriorities[coreID].insert(idx, newPriorities)
        #     self.queueExpectedDurationArrays[coreID] = np.insert(self.queueExpectedDurationArrays[coreID], idx, durationChange[2])

        



        if (queueSizeBefore != len(self.jobQueue[coreID])):
            print(queueSizeBefore, len(self.jobQueue[coreID]))
            raise ValueError("Queue was not maintained")

    @classmethod
    def _print_status(cls, event, total):
        percent = 100.0 * event / total
        sys.stdout.write(f"\rFlushing Queue {event}/{total} ({percent:6.2f}%)")
        sys.stdout.flush()
        



