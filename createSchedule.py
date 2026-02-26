#!/usr/bin/env python
from src.AlgoFIFO import AlgoFIFO
from src.AlgoPreemptive import AlgoPreemptive
from src.Scheduler import Scheduler
import pickle
import argparse

def createSchedule():
    parser = argparse.ArgumentParser(description="Turn Simulated Jobs into a Schedule using chosen algorithm")
    parser.add_argument("-i", "--input", 
                              type=str,
                              default = "jobs.pkl",
                              help="Input file name")
    parser.add_argument("-a", "--algorithm", 
                              type=str, 
                              default = "FIFO", 
                              help="algorithm used to schedule jobs")

    args = parser.parse_args()
    with open(args.input, "rb") as f:
        jobList = pickle.load(f)

    algoName = args.algorithm
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

