

class Segment:

    def __init__(self, segmentID, startTime, endTime, jobID):
        self.segmentID = segmentID
        self.startTime = startTime
        self.endTime = endTime
        self.jobID = jobID
    
    def dump(self):
        print(f"Segment ID: {self.segmentID:5} of JobID: {self.jobID:5}")
        print(f"Starts At: {self.startTime:8.3f} and Finishes at {self.endTime:8.3f}")
        print()

