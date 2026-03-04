from enum import Enum
from collections import deque
from dotenv import load_dotenv
import os

load_dotenv()
floatPrecision = float(os.getenv("FLOAT_PRECISION"))


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
        if abs(self.time - time) > floatPrecision:
            return False
        return True
class Semaphore:

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
        if globalTime < self.previousTime - floatPrecision:
            print(globalTime, self.previousTime)
            raise ValueError("Jobs were not submitted in order")
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
        if globalTime < self.previousTime - floatPrecision:
            raise ValueError("Jobs were not submitted in order")
        self.waitOperations.append(WaitOperation(globalTime, self.previousValue, jobID, threadID, subThreadID))
        val = self.previousValue
        self.previousValue -= 1
        self.previousTime = globalTime
        if val <= 0:
            self.waiting.append((jobID, threadID, subThreadID))
            return True
        return False      

    def removeWait(self, globalTime, jobID, threadID, subThreadID, removedThreadStartCondition):
        if removedThreadStartCondition != SemOperation.Wait:
            raise ValueError("Can only change semaphore if reemoving a waiting thread")
        self.previousValue += 1
        if (jobID, threadID, subThreadID) in self.waiting:
            self.waiting.remove((jobID, threadID, subThreadID))

        removedWaitOperation = False
        for waitOperation in self.waitOperations:
            if waitOperation.checkIfEqual(globalTime, jobID, threadID, subThreadID):
                self.waitOperations.remove(waitOperation)
                removedWaitOperation = True
                break

        if not removedWaitOperation:
            raise ValueError("Could not find matching wait operation to remove")

        for i, postOperation in enumerate(self.postOperations):
            if postOperation[0] > globalTime + floatPrecision:
                time, value = postOperation
                self.postOperations[i] = (time, value+1)

        for i, waitOperation in enumerate(self.waitOperations):
            if waitOperation.time > globalTime + floatPrecision:
                waitOperation.value += 1
            
        
