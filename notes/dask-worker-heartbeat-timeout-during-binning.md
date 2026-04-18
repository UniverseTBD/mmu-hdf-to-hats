# Dask worker heartbeat timeouts during HATS Binning stage

## Symptom

Partway through a MaNGA HATS build (`jobs/build-manga.sbatch`, 96 workers), the
log fills with ~90 lines of:

```
distributed.scheduler - WARNING - Worker failed to heartbeat for 308-310s;
attempting restart: <WorkerState 'tcp://127.0.0.1:...', name: N,
status: running, memory: M, processing: 0>
```

followed by matching `distributed.nanny - WARNING - Restarting worker` lines and
many `Removing worker ... caused the cluster to lose already computed task(s),
which will be recomputed elsewhere: {map_to_pixels-...}` entries.

All of this happens between the `Catalog: Mapping` bar finishing (100%) and the
`Catalog: Binning` bar finishing (2/2). The job does **not** crash — Splitting
begins normally afterwards, though the first several Splitting iterations are
very slow (~180 s/it tapering down) as lost mapping outputs are recomputed.

## Why it happens

Every worker tripped the heartbeat timeout within ~2 seconds of each other, and
all of them had `processing: 0`. That means the workers were idle and alive —
the **scheduler's event loop** was blocked for ~5 minutes and simply could not
service heartbeats.

The blocking work is the `Catalog: Binning` step in hats/LSDB. After Mapping
produces per-input pixel histograms, Binning consolidates them on the
client/scheduler to decide the final pixel layout. For a 477-input catalog at
our chosen pixel thresholds this is a single long synchronous operation on one
process. Default heartbeat TTL is 300 s, so anything past that marks every
worker as dead.

## Why the job survived

1. Workers are launched under **nannies**, which auto-restart them when the
   scheduler kills them (`distributed.nanny - WARNING - Restarting worker`).
2. The scheduler reschedules the lost `map_to_pixels-*` futures that those
   workers were holding in memory — it treats them as failed tasks and
   recomputes them elsewhere.
3. The driver process is just awaiting the final future, so the restart is
   transparent to it.

The cost is purely wasted compute: every mapping result that was sitting in
worker memory at heartbeat-death time has to be recomputed before Splitting
can proceed on that partition. That is what the long leading Splitting
iterations are.

## Fixes / mitigations (if it becomes a problem)

We didn't change anything this run because the job completed, but options:

- Raise the worker TTL so the stall is tolerated:
  `DASK_DISTRIBUTED__SCHEDULER__WORKER_TTL=30min` (or set in the Dask config
  file / `dask.config.set`). Heartbeat interval itself is 5 s; the TTL is what
  we want to bump.
- Also consider `distributed.comm.timeouts.connect` / `.tcp` if the scheduler
  restart storm causes secondary connection timeouts.
- Structural fix: reduce the work the scheduler has to do in Binning — either
  fewer input partitions going in, or a coarser pixel threshold so the
  histogram consolidation is cheaper. This is the real root cause; the TTL
  bump just hides it.

## How to recognise this vs. a real worker failure

Key tells that it's the scheduler-stall pattern and not actual worker crashes:

- **All** workers fail within a ~1–2 s window (real failures are staggered).
- Every failing worker shows `processing: 0` (they were idle, not OOM-killed).
- Heartbeat ages cluster tightly around 308–310 s (the TTL), not random values.
- Happens at a stage boundary (Mapping→Binning, Binning→Splitting) where the
  scheduler is known to do synchronous work.

If instead you see staggered failures, non-zero `processing`, or varied
heartbeat ages, it's more likely a genuine worker problem (OOM, disk, etc.)
and the TTL bump will not help.
