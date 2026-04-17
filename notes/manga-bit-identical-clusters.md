# MaNGA: bit-identical coordinate clusters

## The issue

The MMU MaNGA ingest is driven by `drpall-v3_1_1.fits` (MANGA HDU), inner-joined
to `dapall` HDU `HYB10-MILESHC-MASTARSSP` on `plateifu`/`PLATEIFU` and filtered
to `DAPDONE == True`. This is what `scripts/manga/build_parent_sample.py` in
the MMU repo does and it matches the row count in the staging histogram
(~10,735 rows; we measure 10,737 from the catalog filter — see
`notebooks/check_manga_drpall_collisions_mmu.ipynb`).

Each row in drpall is one IFU observation (`plateifu`), but a single galaxy
(`mangaid`) can be observed by more than one IFU at identical
`(objra, objdec)`. Running the collision sweep on the *MMU parent sample*
(post-DAPDONE filter):

- 10,737 total rows across 10,556 unique `mangaid`
- **146 bit-identical `(objra, objdec)` groups** containing 319 rows in total
- **Max 5 rows at a single coordinate**

(The unfiltered drpall has noticeably worse clustering — 169 groups, max 49 —
because the 49-row cluster is a calibration/standards target whose
re-observations are excluded by `DAPDONE`. Always do the analysis on the
MMU-filtered sample.)

At any healpix order, these clusters occupy the same pixel by construction
(HEALPix is a function of RA/Dec; bit-identical coords always land in the
same pixel). The sweep on the MMU sample plateaus at `max_per_pixel = 5`
from order 13 onward and `n_collisions` stabilizes near 146 at high orders.

## Why HATS cannot solve this at the format level

HATS's invariant is: healpix leaf pixel ↔ exactly one Parquet file. The
spatial index `ang2pix(nside, ra, dec) → file` must be a function for
downstream tools (LSDB cone search, xmatch, margin cache) to work. If a
single pixel held multiple files, the index would no longer be unique.

`row_group_size` splits a file's internal layout but doesn't produce multiple
files per pixel.

So the lower bound on file size is *the largest bit-identical cluster*.
For the MMU MaNGA sample that is 5 reobservations of the same galaxy in one
file (was 49 before we filtered correctly).

## Current configuration

The sbatch job runs with `--max-rows=6 --highest-healpix-order=12`:

- At order 12 the MMU sample has `max_per_pixel = 6`, so `max-rows >= 6`
  satisfies hats-import's `pixel_threshold` check natively — no patching
  required.
- We previously monkey-patched `hats.pixel_math.partition_stats` to bypass
  the validation; that patch has been removed now that the parent sample is
  small enough.
- The 146 unsplittable clusters each produce one file containing 2-5 rows;
  splittable rows go into pixels with up to 6 rows.

## More involved alternative: object/source split

HATS's native pattern for many-observations-per-object is a three-catalog
setup using `hats_import.association.AssociationArguments`:

```
object_catalog/   # one row per mangaid at (objra, objdec) — ~10,556 rows
source_catalog/   # one row per plateifu — 10,737 rows, carries the payload
assoc_obj_src/    # foreign-key mapping: mangaid ↔ plateifu
```

The *object* catalog has no coordinate collisions and can be partitioned
aggressively (one file per row). The *source* catalog can be partitioned by a
different column (for example `plateifu` hash, or source_id) so identical
coordinates no longer force co-location in one file — the partitioning is no
longer spatial.

Why we did not pursue this now:

- It doubles the pipeline: two imports plus an association pass.
- The MaNGA scientific unit that users query is the `plateifu` observation
  (one IFU → one cube + DAP maps), not the collapsed galaxy. An object
  catalog adds a layer users mostly don't need.
- After the DAPDONE filter only ~1.7% of galaxies are reobserved and the
  worst cluster is 5 rows — small enough that flat partitioning is fine.
- LSDB users who want per-galaxy grouping can call `.nest_lists('mangaid')`
  on the flat catalog after loading.

If per-object file size turns out to dominate, or if a future MaNGA release
has significantly more reobservations, revisit the association-catalog
approach.
