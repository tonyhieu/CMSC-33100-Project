from .Thread import Thread

class Job:

    def __init__(self, jobID, 
                       submissionTime, 
                       nThreads, 
                       threads=None, 
                       actualLengths=None, 
                       expectedLengths=None,
                       semPosts=None,
                       semWaits=None,
                       synchronizedThreads=None,
                       demandFunction=None):#for each thread, list the threads that cannot share the same core because they are synchronized
        """
        constructor either needs a list of threads or
        lists of actualLengths, expectedLengths, semPosts, and semWaits
        """
        self.id = jobID
        self.nThreads = nThreads
        self.submissionTime = submissionTime
        self.demandFunction = demandFunction

        threads = [] if threads is None else threads
        actualLengths = [] if actualLengths is None else actualLengths
        expectedLengths = [] if expectedLengths is None else expectedLengths
        semPosts = [] if semPosts is None else semPosts
        semWaits = [] if semWaits is None  else semWaits
        synchronizedThreads = [] if synchronizedThreads is None else synchronizedThreads

        if len(threads) > 0:
            if len(threads) != nThreads:
                raise ValueError("the list threads must have length nThreads")
            if len(synchronizedThreads) > 0 and len(synchronizedThreads) != nThreads:
                raise ValueError("the list synchronizedThreads must have length nThreads")
            if len(synchronizedThreads) > 0:
                self.synchronizedThreads = synchronizedThreads
            else:
                self.synchronizedThreads = [[] for _ in range(nThreads)]
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

    def getTotalExpectedLength(self):
        return sum(thread.expectedLength for thread in self.threads)

    def getDemandAt(self, allocation):
        if allocation < 1:
            raise ValueError("allocation must be >= 1")
        demandFunction = getattr(self, "demandFunction", None)
        if demandFunction:
            nearest = max((a for a in demandFunction if a <= allocation), default=None)
            if nearest is not None:
                return demandFunction[nearest]
        return self.getTotalExpectedLength() / allocation

    def getDemandMin(self):
        return self.getDemandAt(1)

    def getEfficiencyAt(self, allocation):
        # zeta(n) = demand_min / (n * demand(n))
        return self.getDemandMin() / (allocation * self.getDemandAt(allocation))

    def getAllocationCap(self, maxAllocation, zetaMin):
        if maxAllocation < 1:
            return 1
        cap = 1
        for allocation in range(1, maxAllocation + 1):
            if self.getEfficiencyAt(allocation) >= zetaMin:
                cap = allocation
            else:
                break
        return cap
