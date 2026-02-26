class Thread:
    def __init__(self, threadID, jobID, actualLength, expectedLength, submissionTime):
        self.threadID = threadID
        self.jobID = jobID
        self.actualLength = actualLength
        self.expectedLength = expectedLength
        self.submissionTime = submissionTime

    def dump(self):
        print(f"---Thread {self.threadID:5} Has Length: {self.actualLength:8.3f} and Expected Length: {self.expectedLength:8.3f}")