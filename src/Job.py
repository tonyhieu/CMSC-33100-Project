from .Thread import Thread

class Job:

    def __init__(self, jobID, submissionTime, nThreads, threads = [], actualLengths = [], expectedLengths = []):
        """
        constructor either needs a list of threads or a list of 
        actual inteervaal lengths and expected interval lengths
        """
        self.id = jobID
        self.nThreads = nThreads
        self.submissionTime = submissionTime
        if len(threads) > 0:
            if len(threads) != nThreads:
                raise ValueError("the list threads must have length nThreads")
            self.threads = threads
        else:
            if len(expectedLengths) != nThreads:
                raise ValueError("the list expectedLengths must have length nThreads")
            if len(actualLengths) != nThreads:
                raise ValueError("the list actualLengths must have length nThreads")
            self.threads = [Thread(threadID, jobID, actualLength, expectedLength, submissionTime)
                            for threadID, actualLength, expectedLength
                            in zip(range(self.nThreads), actualLengths, expectedLengths)]


    def dump(self):
        print(f"***Job {self.id:5} Submitted at time: {self.submissionTime:8.3f}***")
        print(f"-------------Has {self.nThreads:5} Threads")
        for thread in self.threads:
            thread.dump()
            

