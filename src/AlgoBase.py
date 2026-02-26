from abc import ABC, abstractmethod
from src import Schedule

class AlgoBase(ABC):

    def __init__(self, algoType, nCores):
        self.algoType = algoType
        self.nCores = nCores
        self.currentSchedule = Schedule.Schedule(nCores)
        self.scheduledJobs = {} #Dictionary of {jobID: ScheduledJob}

    @abstractmethod
    def handleJobSubmission(self, job):
        pass

    @abstractmethod
    def evaluateSchedule(self):
        pass