

class Segment:

    def __init__(self, segmentID, jobID, coreID, threadID, startTime, endTime, expectedDuration):
        self.segmentID = segmentID
        self.startTime = startTime
        self.endTime = endTime
        self.jobID = jobID
        self.coreID = coreID
        self.threadID = threadID
        self.expectedDuration = expectedDuration
    
    def dump(self):
        print(f"\t\tSegment ID: {self.segmentID:5}, Thread ID: {self.threadID:5} of JobID: {self.jobID:5} running on Core: {self.coreID:5}")
        print(f"\t\tStarts At: {self.startTime:8.3f} and Finishes at {self.endTime:8.3f}")
        print()

