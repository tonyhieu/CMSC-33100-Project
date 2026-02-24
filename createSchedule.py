#!/usr/bin/env python
from src import AlgoFIFO
from src import Scheduler
import pickle

def createSchedule():
    with open("jobs.pkl", "rb") as f:
        jobList = pickle.load(f)

    algo = AlgoFIFO.AlgoFIFO()
    scheduler = Scheduler.Scheduler(algo, jobList)
    
    scheduler.createSchedule()
    schedulePreformance = scheduler.evaluateSchedule()
    schedulePreformance.dump()

if __name__ == "__main__":
    createSchedule()

