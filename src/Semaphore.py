from enum import Enum
import numpy as np
from collections import deque

class SemOperation(Enum):
    Post = "Post"
    Wait = "Wait"
    Blank = "Blank" #inlude this because every segment will start and with either a post, wait, or neither

class PostResult:

    def __init__(self, freeing, coreID, entryID):
        self.freeing = freeing
        self.coreID = coreID
        self.entryID = entryID

class Semaphore:
    floatPrecision = 1e-3

    def __init__(self, semID, initValue, jobID):
        self.semID = semID
        self.jobID = jobID
        self.previousValue = initValue
        self.previousTime = 0.0
        self.timeIdx = -1
        self.waiting = deque([])
        self.postOperations = []
        self.waitOperations = []

    def postAtTime(self, globalTime):
        if globalTime < self.previousTime:
            raise ValueError("Jobs were not submited in order")
        self.postOperations.append(globalTime)
        value = self.previousValue
        self.previousValue += 1
        self.previousTime = globalTime
        if value < 0:
            freedSegment = self.waiting.popleft()
            return PostResult(True, freedSegment[0], freedSegment[1])
        return PostResult(False, -1, -1)

    def waitAtTime(self, globalTime, coreID, entryID):
        if globalTime < self.previousTime:
            raise ValueError("Jobs were not submited in order")
        self.waitOperations.append(globalTime)
        val = self.previousValue
        self.previousValue -= 1
        self.previousTime = globalTime
        if val <= 0:
            self.waiting.append((coreID, entryID))
            return True
        return False
        
        
        

