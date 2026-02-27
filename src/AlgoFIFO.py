from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment
from queue import Queue
import numpy as np

class AlgoFIFO(AlgoBase):

    def __init__(self, nCores):
        super().__init__("FIFO", nCores)
        #we create a seperate queue for each core available to schedule threads on
        self.jobQueue = [Queue() for _ in range(self.nCores)]
        self.queueExpectedDuration = [0.0 for _ in range(self.nCores)]

    def handleJobSubmission(self, job):

        '''
        we place ourselves in the moment of the scheduler right at this current
        jobs submission time 

        first we must add all jobs from queue to the schedule after converting them to threads, until 
        the most recently scheduled threads is currently running (its finish 
        time is after the current jobs submission time)

        the scheduler could be in a waiting state- at the time of submission, all
        threads have finished and nothing in the queue. This is handled by looking at 
        the jobQueue size
        '''
        for coreID in range(self.nCores):
            coreEndTime = self.currentSchedule.getExactEndTime(coreID)
            while ((self.jobQueue[coreID].qsize() > 0) and  (coreEndTime < job.submissionTime)):
                self.scheduleThreadFromQueue(coreID)
                coreEndTime = self.currentSchedule.getExactEndTime(coreID)
        
        '''
        we now add the current jobs threads to the queue and estimate the finish time of the last thread
        '''
        threadExpectedEndTimes = np.zeros(job.nThreads)
        for theadID, thread in enumerate(job.threads):
            #check core with earliest expected start time
            earliestCore = 0
            earliestExpectedStartTime = self.getEarliestExpectedStartTime(job.submissionTime, 0)
            for coreID in range(1, self.nCores):
                coreExpectedStartTime = self.getEarliestExpectedStartTime(job.submissionTime, coreID)
                if coreExpectedStartTime < earliestExpectedStartTime:
                    earliestExpectedStartTime = coreExpectedStartTime
                    earliestCore = coreID
            
            self.jobQueue[earliestCore].put(thread)
            self.queueExpectedDuration[earliestCore] += thread.expectedLength
            threadExpectedEndTimes[theadID] = earliestExpectedStartTime + thread.expectedLength

        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(np.max(threadExpectedEndTimes))
        self.scheduledJobs[job.id] = scheduledJob
        

    def scheduleThreadFromQueue(self, coreID):
        if self.jobQueue[coreID].qsize() == 0:
            raise ValueError("No Jobs in Queue to Schedule")

        threadToSchedule = self.jobQueue[coreID].get()
        self.queueExpectedDuration[coreID] -= threadToSchedule.expectedLength
        threadStartTime = max(self.currentSchedule.getExactEndTime(coreID), 
                           threadToSchedule.submissionTime)
        threadEndTime = threadStartTime + threadToSchedule.actualLength

        segmentID = self.scheduledJobs[threadToSchedule.jobID].getNumberOfScheduledSegments()
        segment = Segment(segmentID, 
                          threadToSchedule.jobID, 
                          coreID, 
                          threadToSchedule.threadID, 
                          threadStartTime, 
                          threadEndTime, 
                          threadToSchedule.expectedLength)
        self.scheduledJobs[threadToSchedule.jobID].addSegment(segment)
        self.currentSchedule.addSegment(segment)    

    def getEarliestExpectedStartTime(self, submissionTime, coreID):
        '''
        the expected start time on this core is either the submission time, or the 
        expected finish time of the last job, whichever is latest
        plus the expected length of the queue
        '''
        return max(submissionTime, self.currentSchedule.getLastJobsExpectedEndTime(coreID)) \
                + self.queueExpectedDuration[coreID]

        


    def evaluateSchedule(self):
        '''
        handleJobSubmission Placed Jobs in the queue, here we have to empty the 
        queue into the schedule before we analyze the schedule
        '''
        for coreID in range(self.nCores):
            while self.jobQueue[coreID].qsize() > 0:
                self.scheduleThreadFromQueue(coreID)


        self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType, self.currentSchedule)
        
        print("\n\n")
        return sp
