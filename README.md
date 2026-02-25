# CMSC-33100-Project


Duncan's work so far:

I built a test framework to test algorithms on simulated jobs

The executable: "simulateJobList.py" simulates as a list of SimulatedJob objects  with variable start times, durations, and duration unceertainty. These are saved to a file so algorithms can run on the same set of simulated jobs

The executable "createSchedule.py" runs an algorithm (custom class inheriteed from AlgoBase) to create a schedule. The schedule consists of segments split throughout time. A segment is a fraction of a job, so each job is split into a finite number of segments that appear in the schedule.

The final product of this schedule is a distionary of {int, ScheduledJob} which has jobID as the key and the scheduled job as the value. I implemented three metrics to measure schedules but I think they can be much improved

To Do:

expand simulation and scheduling to be a function of resourses, cpu, gpu etc. Right now it is just one schedule on one processor

refine the metrics for scheduling jobs

implement the more sophisticated scheduling algorithms, I just have two so far

build a method, probably in the Schedule Class, which plots the schedule so it is visually appealing and we can visuaally check for bugs

refine the simulation process to be as realistc as possible, I just through quick and easy stuff in there

