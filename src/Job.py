

class Job:

    def __init__(self, jobID, submissionTime, intervalLength):
        self.id = jobID
        self.submissionTime = submissionTime
        self.intervalLength = intervalLength


    def dump(self):
        print(f"***Job {self.id:5} Submitted at time: {self.submissionTime:8.3f}***")
        print(f"-------------Has Length: {self.intervalLength:8.3f}")

