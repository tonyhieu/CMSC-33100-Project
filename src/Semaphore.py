from enum import Enum
import numpy as np
from collections import deque

class SemOperation(Enum):
    Post = "Post"
    Wait = "Wait"
    Blank = "Blank" #inlude this because every segment will start and with either a post, wait, or neither

class PostResult:

    def __init__(self, freeing, jobID, threadID, subThreadID):
        self.freeing = freeing
        self.jobID = jobID
        self.threadID = threadID
        self.subThreadID = subThreadID

class WaitOperation:

    def __init__(self, time, value, jobID, threadID, subThreadID):
        self.time = time
        self.value = value
        self.jobID = jobID
        self.threadID = threadID
        self.subThreadID = subThreadID
    
    def checkIfEqual(self, time, jobID, threadID, subThreadID):
        if self.jobID != jobID:
            return False
        if self.threadID != threadID:
            return False
        if self.subThreadID != subThreadID:
            return False
        if abs(self.time - time) > Semaphore.floatPrecision:
            return False
        return True



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
        self.postOperations.append((globalTime, self.previousValue))
        value = self.previousValue
        self.previousValue += 1
        self.previousTime = globalTime
        if value < 0:
            freedSegment = self.waiting.popleft()
            return PostResult(True, freedSegment[0], freedSegment[1], freedSegment[2])
        return PostResult(False, -1, -1, -1)

    def waitAtTime(self, globalTime, jobID, threadID, subThreadID):
        if globalTime < self.previousTime:
            raise ValueError("Jobs were not submited in order")
        self.waitOperations.append(WaitOperation(globalTime, self.previousValue, jobID, threadID, subThreadID))
        val = self.previousValue
        self.previousValue -= 1
        self.previousTime = globalTime
        if val <= 0:
            self.waiting.append((jobID, threadID, subThreadID))
            return True
        return False

    def removeWait(self, globalTime, jobID, threadID, subThreadID):
        self.previousValue += 1
        self.waiting.remove((jobID, threadID, subThreadID))
        for waitOperation in self.waitOperations:
            if waitOperation.time > globalTime:
                waitOperation.value += 1
            if waitOperation.checkIfEqual(globalTime, jobID, threadID, subThreadID):
                self.waitOperations.remove(waitOperation)
                break
        for postOperation in self.postOperations:
            if postOperation[0] > globalTime:
                time, value = postOperation
                postOperation = (time, value+1)
        
        

