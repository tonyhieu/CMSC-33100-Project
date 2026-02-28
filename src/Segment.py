import numpy as np

class Segment:
    

    def __init__(self, segmentID, 
                       coreID, 
                       subThread,
                       startTime, 
                       endTime):
        self.segmentID = segmentID
        self.startTime = startTime
        self.endTime = endTime
        self.jobID = subThread.jobID
        self.coreID = coreID
        self.threadID = subThread.threadID
        self.subThreadID = subThread.subThreadID
        self.expectedDuration = subThread.expectedLength
        self.start = subThread.start
        self.end = subThread.end
        self.waitingTime = 0.0
        self.waiting = False
        self.previousEndTime = -1.0

    def makeWait(self):
        self.waiting = True
        self.previousEndTime = self.endTime
        self.endTime = np.inf

    def resumeAtTime(self, globalTime):
        self.waiting = False
        self.waitingTime = globalTime - self.startTime
        self.endTime = self.waitingTime + self.previousEndTime

    def dump(self):
        print(f"\t\tSegment ID: {self.segmentID:5}, SubThread ID: {self.subThreadID:5}, Thread ID: {self.threadID:5} of JobID: {self.jobID:5} running on Core: {self.coreID:5}")
        print(f"\t\tStarts At: {self.startTime:8.3f} and Finishes at {self.endTime:8.3f}")
        print()

