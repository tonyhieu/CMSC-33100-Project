#!/usr/bin/env python
from src.AlgoFIFO import AlgoFIFO
from src.AlgoPriorityQueue import AlgoPriorityQueue, PriorityType
from src.Scheduler import Scheduler
import pickle
import argparse
from enum import Enum
from src.Verifier import Verifier

class AlgoType(Enum):
    FIFO = "FIFO"
    PriorityQueue = "PriorityQueue"

def createSchedule():
    parser = argparse.ArgumentParser(description="Turn Simulated Jobs into a Schedule using chosen algorithm")
    parser.add_argument("-i", "--input", 
                              type=str,
                              default = "jobs.pkl",
                              help="Input file name")
    parser.add_argument("-a", "--algorithm", 
                              type=AlgoType, 
                              default = "FIFO", 
                              help="algorithm used to schedule jobs")

    parser.add_argument("-n", "--number", 
                              type=int, 
                              default = 3, 
                              help="number of resourses available to run threads")

    args = parser.parse_args()
    with open(args.input, "rb") as f:
        jobList, globalSemaphoreList = pickle.load(f)

    match args.algorithm:
        case AlgoType.PriorityQueue:
            algo = AlgoPriorityQueue(args.number, PriorityType.expectedLength, globalSemaphoreList)
        case AlgoType.FIFO:
            algo = AlgoFIFO(args.number, globalSemaphoreList)

    scheduler = Scheduler(algo, jobList)

    scheduler.createSchedule()
    schedulePreformance = scheduler.evaluateSchedule()

    verifier = Verifier(algo, jobList, globalSemaphoreList)
    if verifier.verified:
        print(f"Algorithm {args.algorithm.name} created a Verified Schedule!")
    schedulePreformance.dump()

if __name__ == "__main__":
    createSchedule()

