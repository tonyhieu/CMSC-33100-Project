from .Semaphore import SemOperation
import numpy as np

class Thread:
    def __init__(self, threadID, 
                       jobID, 
                       actualLength, 
                       expectedLength, 
                       submissionTime,
                       semPosts,
                       semWaits):
        self.threadID = threadID
        self.jobID = jobID
        self.actualLength = actualLength
        self.expectedLength = expectedLength
        self.submissionTime = submissionTime
        tempPosts = [(semPost[0], semPost[1], SemOperation.Post) for semPost in semPosts]
        tempWaits = [(semWait[0], semWait[1], SemOperation.Wait) for semWait in semWaits]
        #list of tuples (semID, time in thread it occurs, operation), sorted by time they occur to easily split into subthreads
        self.semOperations = sorted(tempPosts + tempWaits, key=lambda op: op[1])
        self.subThreads = []

    def numberSemaphoresInWindow(self, windowStart, windowEnd):
        if ((windowStart < 0.0) or (windowStart > windowEnd) or (windowEnd > self.actualLength)):
            raise ValueError("not a valid window")
        if len(self.semOperations) == 0:
            return 0
        count = 0
        for semOp in self.semOperations:
            if (windowStart <= semOp[1]) and (windowEnd > semOp[1]):
                count+=1
        return count

    def splitWindowBySemaphores(self, windowStart, windowEnd, firstSubThreadID):
        if ((windowStart < 0.0) or (windowStart > windowEnd) or (windowEnd > self.actualLength)):
            raise ValueError("not a valid window")
        if len(self.semOperations) == 0:
            raise ValueError("no semaphore operations")
        semaphoresInWindow = []
        numberSemaphoresInWindow = 0
        for semOp in self.semOperations:
            if (windowStart <= semOp[1]) and (windowEnd > semOp[1]):
                semaphoresInWindow.append(semOp)
                numberSemaphoresInWindow += 1
            if windowEnd <= semOp[1]:
                break
        if numberSemaphoresInWindow < 1:
            raise ValueError("No semaphores in window to split")
        actualLength = semaphoresInWindow[0][1] - windowStart
        expectedLength = self.expectedLength * actualLength / self.actualLength
        subthreads = [SubThread(firstSubThreadID, 
                                self.threadID, 
                                self.jobID, 
                                self.submissionTime,
                                actualLength,
                                expectedLength,
                                (-1, -1.0, SemOperation.Blank),
                                semaphoresInWindow[0])]
        for i, semOp in enumerate(semaphoresInWindow[1:]):
            actualLength = semOp[1] - semaphoresInWindow[i][1]
            expectedLength = self.expectedLength * actualLength / self.actualLength
            subthreads.append(SubThread(firstSubThreadID + i + 1, 
                                        self.threadID, 
                                        self.jobID, 
                                        self.submissionTime,
                                        actualLength,
                                        expectedLength,
                                        semaphoresInWindow[i],
                                        semOp))
        actualLength = windowEnd - semaphoresInWindow[-1][1]
        expectedLength = self.expectedLength * actualLength / self.actualLength
        subthreads.append(SubThread(firstSubThreadID + numberSemaphoresInWindow, 
                                    self.threadID, 
                                    self.jobID, 
                                    self.submissionTime,
                                    actualLength,
                                    expectedLength,
                                    semaphoresInWindow[-1],
                                    (-1, -1.0, SemOperation.Blank)))
        return subthreads

    def dump(self):
        print(f"---Thread {self.threadID:5} Has Length: {self.actualLength:8.3f} and Expected Length: {self.expectedLength:8.3f}")
        for semOp in self.semOperations:
            print(f"^^^^^^^^^^^^^^^{semOp[2].name}s Sem {semOp[0]:5} at time: {semOp[1]:8.3f}")

class SubThread:


    def __init__(self, subThreadID, threadID, jobID, submissionTime, actualLength, expectedLength, startCondition, endCondition):
        '''
        because each thread has a start, end, and n semaphore operations mixed in between,
        we will break each threead into a subthread, beginning and ending with either a threead start, thread end, 
        or semaphere opereation with NO semaphore operations in the middle of a subthread.

        Start index of 0 is the start of the main thread
        Start index of i is the ith semaphore opeeration 
        '''

        self.subThreadID = subThreadID
        self.threadID = threadID
        self.jobID = jobID
        self.submissionTime = submissionTime
        self.actualLength = actualLength
        self.expectedLength = expectedLength
        self.start = startCondition
        self.end = endCondition

        '''
        used Only in Preemptive Priority Queue to maintain 
        memory of queue position if forced to go back
        '''
        self.priority = -1.0 
        self.tieBreaker = -1.0

        
    def dump(self):
        print(f"---SubThread {self.subThreadID:5} Of Thread {self.threadID:5} Of Job {self.jobID:5} Has Length: {self.actualLength:8.3f} and Expected Length: {self.expectedLength:8.3f}")
        print(f"Start: {self.start[2].name} To Semaphore {self.start[0]:5} At {self.start[1]:8.3f}")

        
        

