from .Semaphore import SemOperation

class Verifier:
    '''
        This class will be used to ensure the schedules created by each finished
        algorithm are correct.

        By correct I mean:
            1    every thread for every Job eventually runs in the schedule
            2    the list of simulatedJobs and scheduledJobs are equivalent
            3    there are no overlapping segments on an individual core
            4    every scheduled thread runs AFTER it was submitted
            5    Semaphore work as expected

            feel free to add and implement more test conditions
    '''
    floatThreshold = 1e-3

    def __init__(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):
        print("Verifying...")
        self.conditions = [self.one, self.two, self.three, self.four, self.five]
        self.verified = True
        for i, condition in enumerate(self.conditions):
            if not condition(finishedAlgorithm, simulatedJobs, globalSemaphoreList):
                print(f"Verification Condition {i+1:3} Failed")
                self.verified = False
                break
            else:
                print(f"Verification Condition {i+1:3} Passed")

    def one(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):
        '''
        1    every thread for every Job eventually runs in the schedule

        to test this I will initialize a list to false for every thread simulated
        I will then loop through the schedule, when I see a segment running (corresponding to a thread)
        I will set its value to true

        at the end every value should be true
        '''

        threadBooleans = [[False for _ in range(job.nThreads)] for job in simulatedJobs]
        for coreID in range(finishedAlgorithm.nCores):
            for segment in finishedAlgorithm.currentSchedule.schedule[coreID]:
                threadBooleans[segment.jobID][segment.threadID] = True


        return all(all(jobBooleans) for jobBooleans in threadBooleans)

    def two(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):
        '''
        2    the list of simulatedJobs and scheduledJobs are equivalent

        to test this I will loop over the scheduled jobs, and make sure the simulated job has equivalend fields
        '''
        if (len(finishedAlgorithm.scheduledJobs) != len(simulatedJobs)):
            print("lens not matching")
            return False

        for jobID, scheduledJob in finishedAlgorithm.scheduledJobs.items():
            simulatedJob = simulatedJobs[jobID]
            if simulatedJob.id != scheduledJob.id:
                print("id not matching")
                return False
            if simulatedJob.nThreads != scheduledJob.nThreads:
                print("nThreads not matching")
                return False
            if simulatedJob.submissionTime != scheduledJob.submissionTime:
                print("submissionTime not matching")
                return False
            for simulatedThread in simulatedJob.threads:
                scheduledThread = simulatedJob.threads[simulatedThread.threadID]
                if scheduledThread.threadID != simulatedThread.threadID:
                    print("thread threadID not matching")
                    return False
                if scheduledThread.jobID != simulatedThread.jobID:
                    print("thread jobID not matching")
                    return False
                if scheduledThread.actualLength != simulatedThread.actualLength:
                    print("thread actualLength not matching")
                    return False
                if scheduledThread.expectedLength != simulatedThread.expectedLength:
                    print("thread expectedLength not matching")
                    return False
                if scheduledThread.submissionTime != simulatedThread.submissionTime:
                    print("thread submissionTime not matching")
                    return False
            threadExpectedDurations = [0.0 for _ in range(simulatedJob.nThreads)]
            threadActualDurations = [0.0 for _ in range(simulatedJob.nThreads)]
            for scheduledSegment in scheduledJob.scheduledSegments:
                correspondingThread = scheduledJob.threads[scheduledSegment.threadID]
                if correspondingThread.threadID != scheduledSegment.threadID:
                    print("segment threadID not matching")
                    return False
                if correspondingThread.jobID != scheduledSegment.jobID:
                    print("segment jobID not matching")
                    return False
                threadExpectedDurations[scheduledSegment.threadID] += scheduledSegment.expectedDuration
                threadActualDurations[scheduledSegment.threadID] += scheduledSegment.endTime - scheduledSegment.startTime - scheduledSegment.waitigTime
            for simulatedThread in simulatedJob.threads:
                if abs(simulatedThread.expectedLength - threadExpectedDurations[simulatedThread.threadID]) > Verifier.floatThreshold:
                    print("segment expectedDuration not matching: ")
                    return False
                if abs(simulatedThread.actualLength - threadActualDurations[simulatedThread.threadID]) > Verifier.floatThreshold:
                    print("segment actualLength not matching: ", simulatedThread.actualLength, threadActualDurations[simulatedThread.threadID], simulatedThread.threadID, simulatedThread.jobID)
                    return False
        return True

    def three(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):
        '''
        3    there are no overlapping segments on an individual core

        to test this I will loop over every segment in each core.
        the previous segment must end before the current one starts
        '''

        for coreID in range(finishedAlgorithm.nCores):
            prevSegmentEnd = 0.0
            for segment in finishedAlgorithm.currentSchedule.schedule[coreID]:
                if segment.startTime < prevSegmentEnd - Verifier.floatThreshold:
                    print(segment.startTime, prevSegmentEnd)
                    return False
                prevSegmentEnd = segment.endTime

        return True
    
    def four(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):
        '''
        4    every scheduled thread runs AFTER it was submitted

        to test this I will look over every segment, compare to its corresponding job submission time
        '''
        for coreID in range(finishedAlgorithm.nCores):
            for segment in finishedAlgorithm.currentSchedule.schedule[coreID]:
                if segment.startTime < simulatedJobs[segment.jobID].submissionTime - Verifier.floatThreshold:
                    print(segment.startTime, simulatedJobs[segment.jobID].submissionTime)
                    return False
        
        return True

    def five(self, finishedAlgorithm, simulatedJobs, globalSemaphoreList):

        for coreID in range(finishedAlgorithm.nCores):
            for segment in finishedAlgorithm.currentSchedule.schedule[coreID]:
                if segment.start[2] == SemOperation.Wait:
                    semaphore = globalSemaphoreList[segment.start[0]]
                    """
                    check all post operations to make sure one makes sense
                    """
                    validPostOperation = False
                    for postOperation in semaphore.postOperations:
                        if postOperation < segment.startTime:
                            """
                            no other wait operations can occur between them
                            """
                            waitInBetween = False
                            for waitOperation in semaphore.waitOperations:
                                if (waitOperation - segment.startTime) < Verifier.floatThreshold:
                                    #this is the current wait operation 
                                    continue
                                elif (waitOperation > postOperation) and (waitOperation < segment.startTime):
                                    waitInBetween = True
                            if not waitInBetween:
                                if segment.waitigTime == 0:
                                    validPostOperation = True
                        else:
                            """
                            no other wait operations can occur between them
                            """
                            waitInBetween = False
                            for waitOperation in semaphore.waitOperations:
                                if (waitOperation - segment.startTime) < Verifier.floatThreshold:
                                    #this is the current wait operation 
                                    continue
                                elif (waitOperation < postOperation) and (waitOperation > segment.startTime):
                                    waitInBetween = True
                            if not waitInBetween:
                                expectedWaitTime = postOperation - segment.startTime
                                if abs(segment.waitigTime - expectedWaitTime) < Verifier.floatThreshold:
                                    validPostOperation = True
                    if validPostOperation:
                        continue
                    else:
                        return False
        return True