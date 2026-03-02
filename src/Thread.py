from .Semaphore import SemOperation
import bisect
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

    def semaphoreInWindow(self, windowStart, windowEnd):
        if ((windowStart < 0.0) or (windowStart > windowEnd) or (windowEnd > self.actualLength)):
            raise ValueError("not a valid window")
        startIndex = bisect.bisect_left(self.semOperations, (-1.0 *np.inf,windowStart,))
        return startIndex < len(self.semOperations)


    def splitWindowBySemaphores(self, windowStart, windowEnd, firstSubThreadID):
        if ((windowStart < 0.0) or (windowStart > windowEnd) or (windowEnd > self.actualLength)):
            raise ValueError("not a valid window")
        startIndex = bisect.bisect_left(self.semOperations, (-1.0 *np.inf,windowStart,))
        
        if ((startIndex == 0) and (len(thread.semOperations) > 0)):
            #start is the main thread start, end is a semaphore operation
            self.actualLength = thread.semOperations[0][1]
            self.start = (-1, 0.0, SemOperation.Blank)
            self.end = thread.semOperations[startIndex]
        elif ((startIndex == 0) and (len(thread.semOperations) == 0)):
            #start is the main thread start, end is the main thread end
            self.actualLength = thread.actualLength 
            self.start = (-1, 0.0, SemOperation.Blank)
            self.end = (-1, self.actualLength, SemOperation.Blank)
        elif ((startIndex > 0) and (startIndex == len(thread.semOperations))): 
            #start is a semaphore operation and end is the main thread end
            self.actualLength = thread.actualLength - thread.semOperations[startIndex - 1][1]
            self.start = thread.semOperations[startIndex - 1]
            self.end = (-1, self.actualLength, SemOperation.Blank)
        elif ((startIndex > 0) and (startIndex < len(thread.semOperations))):
            #start and end are semaphore operations
            self.actualLength = thread.semOperations[startIndex][1] - thread.semOperations[startIndex - 1][1]
            self.start = thread.semOperations[startIndex - 1]
            self.end = thread.semOperations[startIndex]
        else:
            raise ValueError("Logic in SubThread Corrupt")
        
        self.expectedLength = self.actualLength * thread.expectedLength / thread.actualLength

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
        print(f"Start: {self.start[2].name} To {self.start[0]:5} At {self.start[1]:8.3f}")

        
        

