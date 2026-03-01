import math
import numpy as np
from queue import Queue

from .AlgoBase import AlgoBase
from .SchedulePerformance import SchedulePerformance
from .ScheduledJob import ScheduledJob
from .Segment import Segment


class AlgoPCS(AlgoBase):
    """
    Predictability-Centric Scheduling (PCS).

    Implements the WFQ-based scheduling mechanism described in:
        "When will my ML Job finish? Toward providing Completion Time Estimates
        through Predictability-Centric Scheduling" (OSDI 2024).

    Core mechanism
    ──────────────
    • K queues are created, each with weight  w_k = exp(-k * W)  (normalised).
      Queue 0 targets the smallest jobs and receives the highest weight (most
      cores); queue K-1 targets the largest jobs.

    • Jobs are mapped to a queue based on their size (nThreads):
        job → queue k  if  thresholds[k] ≤ job.nThreads < thresholds[k+1]
      where thresholds[K] = ∞.

    • Within each queue scheduling is FIFO - no unbounded preemption.
      This bounds Pred_err as required by the PCS predictability requirement (R1).

    • Cores are partitioned among queues proportionally to their weights
      (largest-remainder method), giving each queue a guaranteed resource share.
      This provides the flexibility required by (R2).

    • Work conservation: if a queue's own cores are all busy but a core belonging
      to another queue is genuinely idle (no pending threads and already finished),
      the arriving thread borrows that idle core.

    Parameters
    ──────────
    nCores     : total CPU cores available.
    nQueues    : number of WFQ queues (clamped to nCores; default 2).
    W          : weight-decay exponent. Higher W → larger disparity between
                 queue weights (default 1.0).
                 W = 0  → equal weights (Max-Min fair allocation).
                 Large W → queue 0 dominates (near-SJF for small jobs).
    thresholds : list of nQueues lower-bound sizes.  Job with nThreads = s is
                 assigned to queue k if thresholds[k] ≤ s < thresholds[k+1].
                 Defaults to [1, 2, 3, …] so each band covers one size step.
    """

    def __init__(self, nCores, globalSemaphoreList, nQueues=2, W=1.0, thresholds=None):
        nQueues = max(1, min(nQueues, nCores))   # 1 ≤ nQueues ≤ nCores
        super().__init__("PCS", nCores, globalSemaphoreList)
        self.nQueues = nQueues
        self.W = W

        # ── Queue weights: w_k = exp(-k * W), normalised ─────────────────
        raw = [math.exp(-k * W) for k in range(nQueues)]
        total = sum(raw)
        self.queueWeights = [r / total for r in raw]

        # ── Partition cores among queues ──────────────────────────────────
        # Guarantee every queue at least 1 core, then distribute the
        # remaining (nCores - nQueues) cores proportionally by weight.
        alloc = [1] * nQueues
        remaining = nCores - nQueues
        if remaining > 0:
            exact_extra = [w * remaining for w in self.queueWeights]
            extra = [int(e) for e in exact_extra]
            deficit = remaining - sum(extra)
            order = sorted(range(nQueues),
                           key=lambda i: exact_extra[i] - extra[i], reverse=True)
            for i in range(deficit):
                extra[order[i]] += 1
            alloc = [alloc[k] + extra[k] for k in range(nQueues)]
        self.queueCoreAlloc = alloc   # alloc[k] = number of cores owned by queue k

        # Assign contiguous global core IDs to each queue
        self.queueCoreIds = []
        start = 0
        for k in range(nQueues):
            self.queueCoreIds.append(list(range(start, start + alloc[k])))
            start += alloc[k]

        # ── Per-core thread FIFOs, indexed [queue][local_core_idx] ────────
        self.jobQueues = [
            [Queue() for _ in range(alloc[k])]
            for k in range(nQueues)
        ]
        # Track total expected duration queued on each (queue, local_core) slot
        self.queueCoreExpDur = [
            [0.0] * alloc[k]
            for k in range(nQueues)
        ]

        # ── Job-size thresholds for queue assignment ───────────────────────
        if thresholds is not None:
            if len(thresholds) != nQueues:
                raise ValueError(
                    f"thresholds must have length {nQueues}, got {len(thresholds)}.")
            self.thresholds = list(thresholds)
        else:
            # Default: queue k handles jobs with nThreads == k+1;
            # the last queue catches everything with nThreads >= nQueues.
            self.thresholds = list(range(1, nQueues + 1))

    # ── Internal helpers ──────────────────────────────────────────────────

    def _queueForJob(self, job):
        """Return the queue index for this job based on its nThreads size."""
        size = job.nThreads
        # Scan from the highest queue downward; assign to the first queue
        # whose threshold is ≤ job size.
        for k in range(self.nQueues - 1, 0, -1):
            if size >= self.thresholds[k]:
                return k
        return 0

    def _globalCoreID(self, queueID, localIdx):
        return self.queueCoreIds[queueID][localIdx]

    def _expectedStartTime(self, queueID, localIdx, submissionTime):
        """
        Estimated time at which the next thread placed on slot (queueID, localIdx)
        would actually begin execution.  Mirrors AlgoFIFO.getEarliestExpectedStartTime.
        """
        coreID = self._globalCoreID(queueID, localIdx)
        base = max(submissionTime,
                   self.currentSchedule.getLastJobsExpectedEndTime(coreID))
        return base + self.queueCoreExpDur[queueID][localIdx]

    def _scheduleFromSlot(self, queueID, localIdx):
        """
        Pop the head thread from slot (queueID, localIdx) and append a Segment
        to the schedule.
        """
        q = self.jobQueues[queueID][localIdx]
        if q.qsize() == 0:
            raise ValueError(f"Slot ({queueID}, {localIdx}) is empty.")

        thread = q.get()
        self.queueCoreExpDur[queueID][localIdx] -= thread.expectedLength

        coreID = self._globalCoreID(queueID, localIdx)
        startTime = max(self.currentSchedule.getExactEndTime(coreID),
                        thread.submissionTime)
        endTime = startTime + thread.actualLength

        segID = self.scheduledJobs[thread.jobID].getNumberOfScheduledSegments()
        seg = Segment(segID, coreID, thread, startTime, endTime)
        self.scheduledJobs[thread.jobID].addSegment(seg)
        self.currentSchedule.addSegment(seg)

    def _isTrulyIdle(self, queueID, localIdx, submissionTime):
        """
        A slot is truly idle if its core has finished all work AND its FIFO has
        no pending threads.  Such a slot can be "borrowed" for work conservation.
        """
        coreID = self._globalCoreID(queueID, localIdx)
        return (self.currentSchedule.getExactEndTime(coreID) <= submissionTime
                and self.jobQueues[queueID][localIdx].qsize() == 0)

    # ── Main scheduling logic ─────────────────────────────────────────────

    def handleJobSubmission(self, job):
        """
        Called in arrival order for every job.  Mirrors the structure of
        AlgoFIFO.handleJobSubmission but uses WFQ queue routing.
        """
        # ── Step 1: advance all per-core schedules up to submissionTime ───
        for queueID in range(self.nQueues):
            for localIdx in range(self.queueCoreAlloc[queueID]):
                coreID = self._globalCoreID(queueID, localIdx)
                endTime = self.currentSchedule.getExactEndTime(coreID)
                while (self.jobQueues[queueID][localIdx].qsize() > 0
                       and endTime < job.submissionTime):
                    self._scheduleFromSlot(queueID, localIdx)
                    endTime = self.currentSchedule.getExactEndTime(coreID)

        # ── Step 2: map job → WFQ queue ───────────────────────────────────
        qID = self._queueForJob(job)

        # ── Step 3: assign each thread to the best available slot ─────────
        # Primary slots: those owned by queue qID (FIFO guarantee).
        # Work conservation: if any slot in another queue is *truly idle*
        # (core finished + no pending threads) and would start earlier, use it.
        threadExpectedEndTimes = np.zeros(job.nThreads)

        coreRestrictions = {}
        for tIdx, thread in enumerate(job.threads):
            badCores = coreRestrictions[tIdx] if tIdx in coreRestrictions else []

            # Evaluate all of qID's own slots
            bestQID = -1
            bestLocal = -1
            bestEst = np.inf
            for lc in range(self.queueCoreAlloc[qID]):
                coreID = self._globalCoreID(qID, lc)
                if coreID in badCores:
                    continue
                est = self._expectedStartTime(qID, lc, job.submissionTime)
                if est < bestEst:
                    bestEst = est
                    bestLocal = lc
                    bestQID = qID

            # Work conservation: check idle slots in other queues
            for otherQID in range(self.nQueues):
                if otherQID == qID:
                    continue
                for lc in range(self.queueCoreAlloc[otherQID]):
                    coreID = self._globalCoreID(otherQID, lc)
                    if coreID in badCores:
                        continue
                    if self._isTrulyIdle(otherQID, lc, job.submissionTime):
                        est = self._expectedStartTime(otherQID, lc,
                                                      job.submissionTime)
                        if est < bestEst:
                            bestEst = est
                            bestLocal = lc
                            bestQID = otherQID

            if bestQID < 0 or bestLocal < 0:
                raise ValueError("No legal core available for thread assignment")

            chosenCoreID = self._globalCoreID(bestQID, bestLocal)
            for syncedThread in job.synchronizedThreads[tIdx]:
                if syncedThread in coreRestrictions:
                    coreRestrictions[syncedThread].append(chosenCoreID)
                else:
                    coreRestrictions[syncedThread] = [chosenCoreID]

            thread.subThreads = self.breakThreadIntoSubThreads(thread)
            for subthread in thread.subThreads:
                self.jobQueues[bestQID][bestLocal].put(subthread)
            self.queueCoreExpDur[bestQID][bestLocal] += thread.expectedLength
            threadExpectedEndTimes[tIdx] = bestEst + thread.expectedLength

        # ── Step 4: register the job with its predicted finish time ───────
        scheduledJob = ScheduledJob(job)
        scheduledJob.setExpectedFinishTime(float(np.max(threadExpectedEndTimes)))
        self.scheduledJobs[job.id] = scheduledJob

    def evaluateSchedule(self, verbose=False):
        """
        Drain all per-core queues into the schedule, then compute and return
        a SchedulePerformance object.
        """
        for queueID in range(self.nQueues):
            for localIdx in range(self.queueCoreAlloc[queueID]):
                while self.jobQueues[queueID][localIdx].qsize() > 0:
                    self._scheduleFromSlot(queueID, localIdx)

        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType,
                                 self.currentSchedule, verbose=verbose)
        if verbose:
            print("\n\n")
        return sp
