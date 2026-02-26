# CMSC-33100-Project


Duncan's work so far:

I built a test framework to test algorithms on simulated jobs

The executable: "simulateJobList.py" simulates as a list of SimulatedJob objects 
These jobs consist of variable number of threads that can be run on different cores with variable submission times, durations, and duration uncertainty. These are saved to a file so algorithms can run on the same set of simulated jobs

The executable "createSchedule.py" runs an algorithm (custom class inherited from AlgoBase) to create a schedule. The schedule consists of segments split throughout time and across a given number of cores. A segment is a thread implemented into a schedule

The final product of this schedule is a dictionary of {int, ScheduledJob} which has jobID as the key and the scheduled job as the value.
The scheduled job oblect has a list of every segment it was scheduled at
I implemented three metrics to measure schedules but I think they can be much improved

To Do:

implement Preemptive algo with multiple cores

refine the metrics for scheduling jobs

implement the more sophisticated scheduling algorithms, I just have two so far

build a method, probably in the Schedule Class, which plots the schedule so it is visually appealing and we can visuaally check for bugs

refine the simulation process to be as realistc as possible, I just through quick and easy stuff in there

