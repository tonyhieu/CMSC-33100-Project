from .Segment import Segment
from .Semaphore import SemOperation

class Schedule:

    def __init__(self, nCores, globalSemaphoreList):
        self.nCores = nCores
        self.globalSemaphoreList = globalSemaphoreList
        self.schedule = [[] for _ in range(self.nCores)]
        self.previousSegmentAddTime = 0.0

    def getExactEndTime(self, coreID):
        if len(self.schedule[coreID]) > 0:
            return self.schedule[coreID][-1].endTime
        else:
             return 0.0

    def isCoreBlocked(self, coreID):
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
        print(segment.startTime, self.previousSegmentAddTime)
        if segment.startTime < self.previousSegmentAddTime:
            raise ValueError("Adding Segments not in Consecutive Order")
        self.previousSegmentAddTime = segment.startTime
        startOp = segment.start[2]
        endOp = segment.end[2]
        if startOp == SemOperation.Wait:
            waitResult = self.globalSemaphoreList[segment.start[0]].waitAtTime(segment.startTime, segment.coreID, len(self.schedule[segment.coreID]))
            if waitResult:
                segment.makeWait()
        elif startOp == SemOperation.Post:
            postResult = self.globalSemaphoreList[segment.start[0]].postAtTime(segment.startTime)
            if postResult.freeing:
                self.startWaitingSegment(postResult.coreID, postResult.entryID, segment.startTime)
        self.schedule[segment.coreID].append(segment)
        return len(self.schedule[segment.coreID]) - 1

    def startWaitingSegment(self, coreID, entryID, resumeTime):
        return self.schedule[coreID][entryID].resumeAtTime(resumeTime)

    def dump(self):
        for coreID in range(self.nCores):
            print(f"Schedule on Core {coreID:5}:")
            for j, segment in enumerate(self.schedule[coreID]):
                print(f"\tSegment {j:5} in Schedule:")
                segment.dump()

    def dumpLasts(self):
        for coreID in range(self.nCores):
            print(f"Last Segment on Core {coreID:5}:")
            self.schedule[coreID][-1].dump()