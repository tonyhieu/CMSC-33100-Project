#!/usr/bin/env python
from src.AlgoFIFO import AlgoFIFO
from src.AlgoPreemptive import AlgoPreemptive
from src.Scheduler import Scheduler
import pickle

def createSchedule():
    with open("jobs.pkl", "rb") as f:
        jobList = pickle.load(f)

    algoName = "preemptive"
    if algoName == "preemptive":
        algo = AlgoPreemptive()
    elif algoName == "FIFO":
        algo = AlgoFIFO()

    scheduler = Scheduler(algo, jobList)
    
    scheduler.createSchedule()
    schedulePreformance = scheduler.evaluateSchedule()
    schedulePreformance.dump()

if __name__ == "__main__":
    createSchedule()

