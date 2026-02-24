
class Scheduler:

    def __init__(self, schedulingAlgorithm, jobList):
        self.schedulingAlgorithm = schedulingAlgorithm
        self.jobList = jobList

    def createSchedule(self):
        for job in self.jobList:
            self.schedulingAlgorithm.addJobToSchedule(job)
    
    def evaluateSchedule(self):
        return self.schedulingAlgorithm.evaluateSchedule(self.jobList)