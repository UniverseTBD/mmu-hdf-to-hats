# MaNGA: bit-identical coordinate clusters

## The issue

The MMU MaNGA ingest is driven by `drpall-v3_1_1.fits` (MANGA HDU), inner-joined to
`dapall` on `plateifu`/`PLATEIFU` and filtered to `DAPDONE == True`. This is
what `scripts/manga/build_parent_sample.py` in the MMU repo uses and it matches
the row count in the staging histogram (~10,735 rows).

Each row in drpall is one IFU observation (`plateifu`), but a single galaxy
(`mangaid`) can be observed by more than one IFU at identical `(objra, objdec)`.
Running the collision sweep against drpall:

- 11,273 total rows across 10,632 unique `mangaid`
- **169 bit-identical `(objra, objdec)` groups** containing 828 rows in total
- **Max 49 rows at a single coordinate**

At any healpix order, these clusters occupy the same pixel by construction
(HEALPix is a function of RA/Dec and bit-identical coords always land in the
same pixel). The sweep plateaus at `max_per_pixel = 49` from order 11 onward
and `n_collisions` stabilizes near 169 (= the bit-identical count) at order 19
and above.

## Why HATS cannot solve this at the format level

HATS's invariant is: healpix leaf pixel ↔ exactly one Parquet file. The
spatial index `ang2pix(nside, ra, dec) → file` must be a function for
downstream tools (LSDB cone search, xmatch, margin cache) to work. If a
single pixel held multiple files, the index would no longer be unique.

`row_group_size` splits a file's internal layout but doesn't produce multiple
files per pixel.

## What we had hoped to do: limit file size

The original intent (reflected in the earlier sbatch config:
`--max-bytes=1 --max-rows=1`) was to keep every output file small — ideally
one row per file, so each cube payload lives in its own Parquet.

This is not achievable for MaNGA:

- `pixel_threshold < 49` (either bytes or rows) causes
  `hats.pixel_math.partition_stats._validate_alignment_arguments` to raise
  `ValueError: single pixel ... exceeds threshold` because 49 rows will always
  fall into one pixel.
- Bypassing the validation causes `_get_alignment` to silently drop those 49
  rows (it leaves the leaf pixel as `None` in the alignment array).
- Increasing `highest_healpix_order` does not help: identical coords never
  separate.

So the lower bound on file size is *the largest bit-identical cluster*.
For MaNGA that is ~49 reobservations of the same galaxy stored in one file.

## Current workaround

Monkey-patch `hats.pixel_math.partition_stats` in `main.py` so that:

1. `_validate_alignment_arguments` skips the `max_bin > threshold` check.
2. `_get_alignment` accepts any non-empty pixel as a leaf when
   `read_order == highest_healpix_order`, regardless of its count.

With `--max-rows=1 --highest-healpix-order=19`:

- Every splittable row lands in its own file.
- The ~169 unsplittable clusters each produce one file containing 2-49 rows.
- No data loss, no validation errors, spatial index still correct.

This is what the sbatch build uses.

## More involved alternative: object/source split

HATS's native pattern for many-observations-per-object is a three-catalog
setup using `hats_import.association.AssociationArguments`:

```
object_catalog/   # one row per mangaid at (objra, objdec) — ~10,632 rows
source_catalog/   # one row per plateifu — 11,273 rows, carries the payload
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
- Only ~6% of galaxies are reobserved; the 49-row cluster appears to be an
  outlier (plausibly repeated pointings of a calibration/standards target),
  not a broad pattern.
- LSDB users who want per-galaxy grouping can call `.nest_lists('mangaid')`
  on the flat catalog after loading.

If per-object file size turns out to dominate, or if a future MaNGA release
has significantly more reobservations, revisit the association-catalog
approach.
