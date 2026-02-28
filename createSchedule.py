#!/usr/bin/env python
from src.AlgoFIFO import AlgoFIFO
from src.AlgoPriorityQueue import AlgoPriorityQueue, PriorityType
from src.AlgoPCS import AlgoPCS
from src.Scheduler import Scheduler
import pickle
from argparse import ArgumentParser
from enum import Enum
from src.Verifier import Verifier

class AlgoType(Enum):
    FIFO = "FIFO"
    PriorityQueue = "PriorityQueue"
    PCS = "PCS"

def createSchedule():
    parser = ArgumentParser(description="Turn Simulated Jobs into a Schedule using chosen algorithm")
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

    parser.add_argument("--nqueues",
                              type=int,
                              default=2,
                              help="(PCS) number of WFQ queues")
    parser.add_argument("--W",
                              type=float,
                              default=1.0,
                              help="(PCS) weight-decay exponent; higher W gives smaller jobs more cores")
    parser.add_argument("--thresholds",
                              type=int,
                              nargs="+",
                              default=None,
                              help="(PCS) per-queue lower-bound job sizes, e.g. --thresholds 1 3 6")

    parser.add_argument("-v", "--verbose",
                              action="store_true",
                              help="show full schedule dump, metrics breakdown, visualization, and verification")

    args = parser.parse_args()
    with open(args.input, "rb") as f:
        jobList, globalSemaphoreList = pickle.load(f)

    match args.algorithm:
        case AlgoType.PriorityQueue:
            algo = AlgoPriorityQueue(args.number, PriorityType.expectedLength, globalSemaphoreList)
        case AlgoType.FIFO:
            algo = AlgoFIFO(args.number, globalSemaphoreList)
        case AlgoType.PCS:
            algo = AlgoPCS(args.number, nQueues=args.nqueues, W=args.W,
                           thresholds=args.thresholds)
    scheduler = Scheduler(algo, jobList)

    scheduler.createSchedule()
    schedulePreformance = scheduler.evaluateSchedule(verbose=args.verbose)

    verifier = Verifier(algo, jobList, globalSemaphoreList)
    if verifier.verified:
        print(f"Algorithm {args.algorithm.name} created a Verified Schedule!")
    if args.verbose:
        schedulePreformance.visualize()
    schedulePreformance.dump()

if __name__ == "__main__":
    createSchedule()

