from abc import ABC, abstractmethod
from src import Schedule
from .Thread import SubThread
from .Semaphore import SemOperation
import numpy as np

class AlgoBase(ABC):

    def __init__(self, algoType, nCores, globalSemaphoreList):
        self.algoType = algoType
        self.nCores = nCores
        self.currentSchedule = Schedule.Schedule(nCores, globalSemaphoreList)
        self.scheduledJobs = {} #Dictionary of {jobID: ScheduledJob}
    
    @classmethod
    def breakThreadIntoSubThreads(cls, thread, nSubThreads):
        if nSubThreads < 1:
            raise ValueError("We need at least one subthread")
        threadBreaks = np.linspace(0.0, thread.actualLength, nSubThreads + 1)
        subThreadID = 0
        subThreadStart = 0.0
        subThreads = []
        for threadBreak in threadBreaks[1:]:
            numberSemaphoresInWindow = thread.numberSemaphoresInWindow(subThreadStart, threadBreak)
            if numberSemaphoresInWindow:
                subThreads += thread.splitWindowBySemaphores(subThreadStart, threadBreak, subThreadID)
                subThreadID += numberSemaphoresInWindow + 1
            else:
                expectedLength = thread.expectedLength * (threadBreak - subThreadStart) / thread.actualLength
                subThreads.append(SubThread(subThreadID, 
                                            thread.threadID, 
                                            thread.jobID, 
                                            thread.submissionTime, 
                                            threadBreak - subThreadStart, 
                                            expectedLength, 
                                            (-1, -1.0, SemOperation.Blank), 
                                            (-1, -1.0, SemOperation.Blank)))
                subThreadID += 1
            subThreadStart = threadBreak
        return subThreads


    @abstractmethod
    def handleJobSubmission(self, job):
        pass

    @abstractmethod
    def evaluateSchedule(self):
        pass