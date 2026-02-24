
class Schedule:

    def __init__(self):
        self.schedule = []

    def getCurrentEndTime(self):
        if len(self.schedule) > 0:
            return self.schedule[-1].endTime
        else:
             return 0.0

    def addSegment(self, segment):
        self.schedule.append(segment)
        return len(self.schedule) - 1

    def replaceLastSegment(self, segment):
        lastSegment = self.schedule[-1]
        self.schedule[-1] = segment
        return lastSegment

    def dump(self):
        for i, segment in enumerate(self.schedule):
            print(f"Segment {i:5} in Schedule:")
            segment.dump()