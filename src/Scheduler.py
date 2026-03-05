import sys

class Scheduler:

    def __init__(self, schedulingAlgorithm, jobList):
        self.schedulingAlgorithm = schedulingAlgorithm
        self.jobList = jobList

    def createSchedule(self):
        for i, job in enumerate(self.jobList):
            Scheduler._print_status(i+1, len(self.jobList))
            self.schedulingAlgorithm.handleJobSubmission(job)
    
    def evaluateSchedule(self, verbose=False):
        return self.schedulingAlgorithm.evaluateSchedule(verbose=verbose)
    
    @classmethod
    def _print_status(cls, event, total):
        percent = 100.0 * event / total
        sys.stdout.write(f"\rProcessing job {event}/{total} ({percent:6.2f}%)")
        sys.stdout.flush()