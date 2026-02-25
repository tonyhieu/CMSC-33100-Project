from abc import ABC, abstractmethod
from src import Schedule

class AlgoBase(ABC):

    def __init__(self, algoType):
        self.algoType = algoType
        self.currentSchedule = Schedule.Schedule()
        self.scheduledJobs = {} #Dictionary of {jobID: ScheduledJob}

    @abstractmethod
    def handleJobSubmission(self, job):
        pass

    @abstractmethod
    def evaluateSchedule(self):
        pass