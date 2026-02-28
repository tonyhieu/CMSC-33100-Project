
class Scheduler:

    def __init__(self, schedulingAlgorithm, jobList):
        self.schedulingAlgorithm = schedulingAlgorithm
        self.jobList = jobList

    def createSchedule(self):
        for job in self.jobList:
            self.schedulingAlgorithm.handleJobSubmission(job)
    
    def evaluateSchedule(self, verbose=False):
        return self.schedulingAlgorithm.evaluateSchedule(verbose=verbose)