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
        if globalTime < self.previousTime - Semaphore.floatPrecision:
            raise ValueError("Jobs were not submited in order")
        self.postOperations.append((globalTime, self.previousValue))
        value = self.previousValue
        self.previousValue += 1
        self.previousTime = globalTime
        if value < 0:
            if len(self.waiting) == 0:
                raise ValueError("Semaphore state is inconsistent: negative value with no waiting threads")
            freedSegment = self.waiting.popleft()
            return PostResult(True, freedSegment[0], freedSegment[1], freedSegment[2])
        return PostResult(False, -1, -1, -1)

    def waitAtTime(self, globalTime, jobID, threadID, subThreadID):
        if globalTime < self.previousTime - Semaphore.floatPrecision:
            raise ValueError("Jobs were not submited in order")
        self.waitOperations.append((globalTime, self.previousValue, jobID, threadID, subThreadID))
        val = self.previousValue
        self.previousValue -= 1
        self.previousTime = globalTime
        if val <= 0:
            self.waiting.append((jobID, threadID, subThreadID))
            return True
        return False

    def removeWait(self, globalTime, jobID, threadID, subThreadID):
        self.previousValue += 1
        if (jobID, threadID, subThreadID) in self.waiting:
            self.waiting.remove((jobID, threadID, subThreadID))

        removedWaitOperation = False
        for waitOperation in self.waitOperations:
            if abs(waitOperation[0] - globalTime) < Semaphore.floatPrecision and \
               waitOperation[2] == jobID and \
               waitOperation[3] == threadID and \
               waitOperation[4] == subThreadID:
                self.waitOperations.remove(waitOperation)
                removedWaitOperation = True
                break

        if not removedWaitOperation:
            raise ValueError("Could not find matching wait operation to remove")

        for i, postOperation in enumerate(self.postOperations):
            if postOperation[0] > globalTime + Semaphore.floatPrecision:
                time, value = postOperation
                self.postOperations[i] = (time, value+1)

        for i, waitOperation in enumerate(self.waitOperations):
            if waitOperation[0] > globalTime + Semaphore.floatPrecision:
                time, value, waitJobID, waitThreadID, waitSubThreadID = waitOperation
                self.waitOperations[i] = (time, value+1, waitJobID, waitThreadID, waitSubThreadID)
            
        

