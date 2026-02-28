# Scheduling Algorithm Performance Tester

This repository contains our final project for CMSC 33100 - Advanced Operating Systems at the University of Chicago. We aimed to implement different scheduling algorithms and judge them based on different performance metrics. The current algorithms implemented are:
- First-in, first-out (FIFO)
- Fixed-priority Preemptive Scheduling (FFPS)
- Predictability-Centric Scheduling (PCS)

## Usage

Clone this repository, install all necessary libraries (contained inside `requirements.txt`), and run `python simulateJobList.py` to create a set of jobs to test (use `-h` to see arguments). Then, run `python createSchedule.py` to run the scheduling algorithm.

## TODO

- [ ] Add more algorithms
- [ ] Add more metrics
- [ ] Make simulation more realistic (e.g. cores can host multiple threads)

