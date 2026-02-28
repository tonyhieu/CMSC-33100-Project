import random
import numpy as np
from .Job import Job
from .Semaphore import Semaphore

class SimulatedJob(Job):

    def __init__(self, jobID, 
                       submissionTime, 
                       nThreads, 
                       expectedLengths, 
                       lengthUncertainty, 
                       syncSemaphoreProbability,
                       mutSemaphoreProbability,
                       globalSemaphoreList):
        actualLengths = []
        for expectedLength in expectedLengths:
            #my first thought to sample only jobs with positive definite length, can probably be improved
            actualLength = 0.0
            while actualLength <= 0:
                actualLength = expectedLength + random.gauss(mu=0.0, sigma=lengthUncertainty)
            actualLengths.append(actualLength)

        '''
        we can use semphores for two types of synchronization:
            1) one thread in job has to wait for another thread in job (call a syncSemaphore)
                in this case we init semaphore to 0 and give the post operation to one thread at one time 
                and give the wait operation to another thread at a different time
                Thus, the second thread connot continue until the first thread finishes
            2) use for mutual exclusion on resources (call a mutSemaphore)
                initialize semaphore to 1 and have one thread wait then after finite time have it post
                same for the other thread
        '''

        '''
        nested list of semaphore posts and waits for each thread, 
        each element is a tuple 
        (semID, time in thread it occurs)
        '''
        semPosts = [[] for _ in range(nThreads)] 
        semWaits = [[] for _ in range(nThreads)]
        '''
        list of thread pairs that requre a seperte core, 
        since sync semaphores cannot be on the same core (potential deadlock)
        '''
        synchronizedThreads = [[] for _ in range(nThreads)]
        if (nThreads > 1):
            useSyncSeemaphore = (random.random() < syncSemaphoreProbability)
            if useSyncSeemaphore:
                semID = len(globalSemaphoreList)
                threadA, threadB = random.sample(range(nThreads), 2) #threadA posts, threadB waits
                threadATime = random.uniform(0, actualLengths[threadA])
                threadBTime = random.uniform(0, actualLengths[threadB])
                syncSem = Semaphore(semID, 0, jobID) #init to 0
                globalSemaphoreList.append(syncSem) 
                semPosts[threadA].append((semID, threadATime))
                semWaits[threadB].append((semID, threadBTime))
                synchronizedThreads[threadA].append(threadB)
                synchronizedThreads[threadB].append(threadA)

            useMutSemaphore = (random.random() < mutSemaphoreProbability)
            if useMutSemaphore:
                semID = len(globalSemaphoreList) + 1
                nThreadsUsing = random.randint(2, nThreads) #number of threads that will use the semaphore
                threadsUsing = random.sample(range(nThreads), nThreadsUsing) #samples thee threadIDs that use mutex
                for threadID in threadsUsing:
                    '''
                    we need to make sure the mutex does not surround any other semaphores
                    deadlock could occur
                    '''
                    waitTime = random.uniform(0.0, actualLengths[threadID] * 3 /4)
                    latestMutexPostTime = actualLengths[threadID]
                    for semPostID, semPostTime in semPosts[threadID]:
                        if semPostTime < waitTime:
                            continue
                        else:
                            latestMutexPostTime = min(semPostTime, latestMutexPostTime)
                    for semPostID, semWaitTime in semWaits[threadID]:
                        if semWaitTime < waitTime:
                            continue
                        else:
                            latestMutexPostTime = min(semWaitTime, latestMutexPostTime)
                    postTime = random.uniform(waitTime, latestMutexPostTime)
                    mutSem = Semaphore(semID, 1, jobID) #init to 1
                    globalSemaphoreList.append(mutSem) 
                    semWaits[threadID].append((semID, waitTime))
                    semPosts[threadID].append((semID, postTime))

        super().__init__(jobID,
                         submissionTime, 
                         nThreads, 
                         actualLengths=actualLengths, 
                         expectedLengths=expectedLengths,
                         semPosts=semPosts,
                         semWaits=semWaits,
                         synchronizedThreads=synchronizedThreads)
