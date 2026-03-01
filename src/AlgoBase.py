from abc import ABC, abstractmethod
from src import Schedule
from .Thread import SubThread

class AlgoBase(ABC):
    floatPrecision = 1e-3

    def __init__(self, algoType, nCores, globalSemaphoreList):
        self.algoType = algoType
        self.nCores = nCores
        self.currentSchedule = Schedule.Schedule(nCores, globalSemaphoreList)
        self.scheduledJobs = {} #Dictionary of {jobID: ScheduledJob}
    
    @classmethod
    def breakThreadIntoSubThreads(cls, thread):
        subThreads = [SubThread(thread, 0)]
        for opID in range(1, len(thread.semOperations) + 1):
            subThreads.append(SubThread(thread, opID))
        return subThreads

    @abstractmethod
    def handleJobSubmission(self, job):
        pass

    @abstractmethod
    def evaluateSchedule(self):
        pass