from .Thread import Thread

class Job:

    def __init__(self, jobID, 
                       submissionTime, 
                       nThreads, 
                       threads = [], 
                       actualLengths = [], 
                       expectedLengths = [],
                       semPosts = [],
                       semWaits = [],
                       synchronizedThreads=[]):#for each thread, list the threads that cannot share the same core because they are synchronized
        """
        constructor either needs a list of threads or
        lists of actualLengths, expectedLengths, semPosts, and semWaits
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
            if len(semPosts) != nThreads:
                raise ValueError("the list semPosts must have length nThreads")
            if len(semWaits) != nThreads:
                raise ValueError("the list semWaits must have length nThreads")
            if len(synchronizedThreads) != nThreads:
                raise ValueError("the list synchronizedThreads must have length nThreads")
            self.synchronizedThreads = synchronizedThreads 
            self.threads = [Thread(threadID, jobID, actualLength, expectedLength, submissionTime, threadSemPosts, threadSemWaits)
                            for threadID, actualLength, expectedLength, threadSemPosts, threadSemWaits
                            in zip(range(self.nThreads), actualLengths, expectedLengths, semPosts, semWaits)]

    def dump(self):
        print(f"***Job {self.id:5} Submitted at time: {self.submissionTime:8.3f}***")
        print(f"-------------Has {self.nThreads:5} Threads")
        for thread in self.threads:
            thread.dump()
            

