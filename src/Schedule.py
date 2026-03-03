from .Semaphore import SemOperation
import os

floatThreshold = float(os.getenv("FLOAT_PRECISION"))

class Schedule:

    def __init__(self, nCores, globalSemaphoreList):
        self.nCores = nCores
        self.globalSemaphoreList = globalSemaphoreList
        self.schedule = [[] for _ in range(self.nCores)]
        self.previousSegmentAddTime = 0.0
        self.waiting = {}

    def getExactEndTime(self, coreID):
        if len(self.schedule[coreID]) > 0:
            return self.schedule[coreID][-1].endTime
        else:
             return 0.0

    def isCoreBlocked(self, coreID):
        '''
        returns the semaphore that is blocking a given core
        -1 is not blocked
        '''
        if len(self.schedule[coreID]) > 0:
            if self.schedule[coreID][-1].waiting:
                return self.schedule[coreID][-1].start[0]
            else:
                return -1
        else:
             return -1

    def getLastJobsExpectedEndTime(self, coreID):
        if len(self.schedule[coreID]) > 0:
            return self.schedule[coreID][-1].startTime + self.schedule[coreID][-1].expectedDuration
        else:
             return 0.0

    def addSegment(self, segment):
        if segment.startTime < self.previousSegmentAddTime - floatThreshold:
            raise ValueError("Adding Segments not in Consecutive Order")
        self.previousSegmentAddTime = segment.startTime
        startOp = segment.start[2]
        endOp = segment.end[2]
        waitResult = False
        if startOp == SemOperation.Wait:
            waitResult = self.globalSemaphoreList[segment.start[0]].waitAtTime(segment.startTime, segment.jobID, segment.threadID, segment.subThreadID)
            if waitResult:
                segment.makeWait()
                self.waiting[(segment.jobID, segment.threadID, segment.subThreadID)] = (segment.coreID, len(self.schedule[segment.coreID]))
        elif startOp == SemOperation.Post:
            postResult = self.globalSemaphoreList[segment.start[0]].postAtTime(segment.startTime)
            if postResult.freeing:
                self.startWaitingSegment(postResult.jobID, postResult.threadID, postResult.subThreadID, segment.startTime)
        self.schedule[segment.coreID].append(segment)
        return waitResult

    def startWaitingSegment(self, jobID, threadID, subThreadID, resumeTime):
        coreID, entryID = self.waiting[(jobID, threadID, subThreadID)]
        self.schedule[coreID][entryID].resumeAtTime(resumeTime)

    def dump(self):
        for coreID in range(self.nCores):
            for segment in self.schedule[coreID]:
                segment.dump()

    def removeLastScheduledSegment(self, coreID):
    
        if len(self.schedule[coreID]) == 0:
            raise ValueError("Trying to Remove segments from empty schedule")
        segment = self.schedule[coreID].pop()
        if not ((segment.jobID, segment.threadID, segment.subThreadID) in self.waiting):
            raise ValueError("trying to remove a segment that is not waiting")
        del self.waiting[(segment.jobID, segment.threadID, segment.subThreadID)]
        if segment.start[2] == SemOperation.Wait:
            self.globalSemaphoreList[segment.start[0]].removeWait(segment.startTime, segment.jobID, segment.threadID, segment.subThreadID, segment.start[2])
        latestStart = 0.0
        for coreSchedule in self.schedule:
            if len(coreSchedule) > 0:
                latestStart = max(latestStart, coreSchedule[-1].startTime)
        self.previousSegmentAddTime = latestStart
        return segment

    def dumpLasts(self):
        for coreID in range(self.nCores):
            print(f"Last Segment on Core {coreID:5}:")
            if len(self.schedule[coreID]) > 0:
                self.schedule[coreID][-1].dump()
            else:
                print("core has nothing scheeduled")