import math
import numpy as np
from collections import deque

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

    def __init__(self, nCores, globalSemaphoreList, nQueues=2, W=1.0, thresholds=None, zetaMin=0.0):
        nQueues = max(1, min(nQueues, nCores))   # 1 ≤ nQueues ≤ nCores
        if not 0.0 <= zetaMin <= 1.0:
            raise ValueError(f"zetaMin must be between 0 and 1, got {zetaMin}")
        super().__init__("PCS", nCores, globalSemaphoreList)
        self.nQueues = nQueues
        self.W = W
        self.zetaMin = zetaMin

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
            [deque() for _ in range(alloc[k])]
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

    def _getPreferredLocalsForJob(self, queueID, job):
        # Allocation cap is computed against the job's own thread count, then clamped to
        # the number of slots this queue actually owns.
        ownSlots = self.queueCoreAlloc[queueID]
        allocationCap = job.getAllocationCap(job.nThreads, self.zetaMin)
        capInQueue = min(allocationCap, ownSlots)
        slotStarts = [
            (self._expectedStartTime(queueID, lc, job.submissionTime), lc)
            for lc in range(ownSlots)
        ]
        slotStarts.sort(key=lambda x: x[0])
        return [lc for _, lc in slotStarts[:capInQueue]], allocationCap

    def _scheduleFromSlot(self, queueID, localIdx):
        """
        Pop the head thread from slot (queueID, localIdx) and append a Segment
        to the schedule.
        """
        q = self.jobQueues[queueID][localIdx]
        if len(q) == 0:
            raise ValueError(f"Slot ({queueID}, {localIdx}) is empty.")

        coreID = self._globalCoreID(queueID, localIdx)
        blockedSemaphore = self.currentSchedule.isCoreBlocked(coreID)
        if blockedSemaphore >= 0:
            return blockedSemaphore

        thread = q.popleft()
        self.queueCoreExpDur[queueID][localIdx] -= thread.expectedLength

        startTime = max(self.currentSchedule.getExactEndTime(coreID),
                        thread.submissionTime)
        endTime = startTime + thread.actualLength
        if not np.isfinite(startTime):
            raise ValueError("Assigning an infinite start time!")

        segID = self.scheduledJobs[thread.jobID].getNumberOfScheduledSegments()
        seg = Segment(segID, coreID, thread, startTime, endTime)
        self.scheduledJobs[thread.jobID].addSegment(seg)
        self.currentSchedule.addSegment(seg)
        return -1

    def _isTrulyIdle(self, queueID, localIdx, submissionTime):
        """
        A slot is truly idle if its core has finished all work AND its FIFO has
        no pending threads.  Such a slot can be "borrowed" for work conservation.
        """
        coreID = self._globalCoreID(queueID, localIdx)
        return (self.currentSchedule.getExactEndTime(coreID) <= submissionTime
                and len(self.jobQueues[queueID][localIdx]) == 0)

    def getSlotNextToBeScheduled(self):
        earliestStartTime = np.inf
        bestQID = -1
        bestLocal = -1
        allQueuesEmpty = True
        
        for queueID in range(self.nQueues):
            for localIdx in range(self.queueCoreAlloc[queueID]):
                q = self.jobQueues[queueID][localIdx]
                if len(q) == 0:
                    continue
                allQueuesEmpty = False
                coreID = self._globalCoreID(queueID, localIdx)
                if self.currentSchedule.isCoreBlocked(coreID) >= 0:
                    continue
                # Peek at the first item
                nextThread = q[0]
                nextThreadStart = max(self.currentSchedule.getExactEndTime(coreID), nextThread.submissionTime)
                if nextThreadStart < earliestStartTime:
                    earliestStartTime = nextThreadStart
                    bestQID = queueID
                    bestLocal = localIdx

        if allQueuesEmpty:
            return -1, -1
        if bestQID < 0:
            return -2, -1
        return bestQID, bestLocal

    # ── Main scheduling logic ─────────────────────────────────────────────

    def handleJobSubmission(self, job):
        """
        Called in arrival order for every job.  Mirrors the structure of
        AlgoFIFO.handleJobSubmission but uses WFQ queue routing.
        """
        # ── Step 1: advance all per-core schedules up to submissionTime ───
        qid, lc = self.getSlotNextToBeScheduled()
        if qid == -2:
            raise ValueError("ALL SEGMENTS ARE BLOCKED")
        while qid >= 0:
            coreID = self._globalCoreID(qid, lc)
            nextCoreEndTime = self.currentSchedule.getExactEndTime(coreID)
            if nextCoreEndTime >= job.submissionTime:
                break
            self._scheduleFromSlot(qid, lc)
            qid, lc = self.getSlotNextToBeScheduled()
            if qid == -2:
                raise ValueError("ALL SEGMENTS ARE BLOCKED")

        # ── Step 2: map job → WFQ queue ───────────────────────────────────
        qID = self._queueForJob(job)

        # ── Step 3: assign each thread to the best available slot ─────────
        # Allocation capping: prefer only the capped number of slots based on
        # zeta(n) = demand_min / (n * demand(n)).
        # Work conservation: if any slot in another queue is *truly idle*
        # (core finished + no pending threads) and would start earlier, use it.
        if self.zetaMin > 0.0 and getattr(job, 'demandFunction', None) is None:
            import warnings
            warnings.warn(
                f"Job {job.id} has no demandFunction; --zetamin has no effect on it.",
                stacklevel=2
            )
        threadExpectedEndTimes = np.zeros(job.nThreads)
        preferredLocals, allocationCap = self._getPreferredLocalsForJob(qID, job)
        preferredSet = set(preferredLocals)

        # Track unique (queueID, localIdx) slots already given to this job so we
        # can enforce the global allocationCap across own-queue AND cross-queue slots.
        assignedSlots = set()

        coreRestrictions = {}
        for tIdx, thread in enumerate(job.threads):
            badCores = set(coreRestrictions.get(tIdx, []))

            # Primary policy: enforce allocation cap by searching only preferred
            # own-queue slots + truly-idle cross-queue slots (up to allocationCap total).
            # Fallback policy: if sync restrictions block all primary choices,
            # allow non-preferred own-queue slots and non-idle cross-queue slots.
            primaryCandidates = []
            fallbackCandidates = []

            slotsUsed = len(assignedSlots)

            for checkQID in range(self.nQueues):
                for checkLC in range(self.queueCoreAlloc[checkQID]):
                    coreID = self._globalCoreID(checkQID, checkLC)
                    if coreID in badCores:
                        continue

                    est = self._expectedStartTime(checkQID, checkLC, job.submissionTime)

                    slotKey = (checkQID, checkLC)
                    alreadyMine = slotKey in assignedSlots

                    isPolicyTarget = (
                        (checkQID == qID and checkLC in preferredSet)
                        or self._isTrulyIdle(checkQID, checkLC, job.submissionTime)
                    )

                    if alreadyMine or (isPolicyTarget and slotsUsed < allocationCap):
                        primaryCandidates.append((est, checkQID, checkLC))
                    else:
                        fallbackCandidates.append((est, checkQID, checkLC))

            candidates = primaryCandidates if primaryCandidates else fallbackCandidates
            if not candidates:
                raise ValueError(f"No legal core available for thread assignment (job {job.id}, thread {tIdx})")

            bestEst, bestQID, bestLocal = min(candidates)

            chosenCoreID = self._globalCoreID(bestQID, bestLocal)
            for syncedThread in job.synchronizedThreads[tIdx]:
                coreRestrictions.setdefault(syncedThread, []).append(chosenCoreID)

            assignedSlots.add((bestQID, bestLocal))
            thread.subThreads = self.breakThreadIntoSubThreads(thread, 10)
            for subthread in thread.subThreads:
                self.jobQueues[bestQID][bestLocal].append(subthread)
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
        qid, lc = self.getSlotNextToBeScheduled()
        if qid == -2:
            raise ValueError("ALL SEGMENTS ARE BLOCKED")
        while qid >= 0:
            self._scheduleFromSlot(qid, lc)
            qid, lc = self.getSlotNextToBeScheduled()
            if qid == -2:
                raise ValueError("ALL SEGMENTS ARE BLOCKED")

        if verbose:
            self.currentSchedule.dump()
        sp = SchedulePerformance(self.scheduledJobs, self.algoType,
                                 self.currentSchedule, verbose=verbose)
        if verbose:
            print("\n\n")
        return sp
