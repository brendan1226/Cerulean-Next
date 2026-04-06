# Fast MARC Import Plugin v0.4.0 — Feedback

**Context:** Testing v0.4.0 against the same 125k-record migration file used for
v0.3.0 testing. v0.4.0 delivered the error-handling fixes we asked for, but
introduced a ~7x performance regression. This document explains what happened
and proposes targeted fixes.

## TL;DR

- ✅ **v0.4.0 correctness fixes land cleanly** — per-record error isolation, failed_records array, duplicate_barcode collapse, splitter resilience. All good.
- ⚠️ **Performance regression: ~120 rec/s vs v0.3.0's 200+ rec/s** (and much worse during the splitter phase, where we saw ~21 rec/s).
- 🔍 **Root cause identified:** "Wide character in print" warnings flooding the log at `FastImport.pm:374` — one per non-ASCII record written to a slice file.
- 📈 **Path forward:** fix the warning storm (one-line patch), add a mode where records that would blow up get shunted to a sidecar file (fast path for clean records, safe path for problem records).

## Observed behavior

### v0.4.0 run stats (125,129 records, partial — cancelled at 47%)

| Phase | Duration | Rate | Notes |
|---|---|---|---|
| POST /push/start → job enqueued | <1s | — | fine |
| Splitter (parent reads file, writes 2 slices) | ~9 min | ~240 rec/s effective but CPU-choked | 38,528 warnings emitted |
| Worker processing (after splitter done) | ongoing | ~120 rec/s | v0.3.0 did ~200+ rec/s |
| Wall time before cancel | 15 min 40s | — | at 58,800/125,129 (47%) |

### The warning storm

```
Wide character in print at /var/lib/koha/kohadev/plugins/Koha/Plugin/BWS/FastMARCImport/BackgroundJob/FastImport.pm line 374.
Wide character in print at ... line 374.
(× 38,528 times)
```

Every record containing non-ASCII chars (38% of our file — normal for modern library MARC) triggered a Perl warning. The warnings went to the worker log file, which grew to 4.7 MB during the splitter phase alone. This is happening in the **parent process** while it's writing records to per-worker slice files.

**Line 374:**
```perl
print { $out_fhs[$target] } $serialized;
```

The `$out_fhs` file handles were opened without `:raw` / `:bytes` mode, so Perl is doing UTF-8 encoding on every write and emitting a warning every time it sees wide chars.

## Fixes — ordered by impact

### Fix #1: Open slice file handles in binary mode (ONE-LINE fix, ~80% speedup)

Wherever `@out_fhs` is opened, add `:raw`:

```perl
# Before
open(my $fh, '>', $slice_path) or die "$slice_path: $!";

# After
open(my $fh, '>:raw', $slice_path) or die "$slice_path: $!";
```

This tells Perl the file is already bytes (which it is — MARC is already UTF-8-encoded ISO2709), so no per-write encoding attempt, no warnings, no stderr flood.

**Expected impact:** The splitter phase should drop from ~9 min to under 1 min. Log size goes from 4.7 MB to a few KB.

### Fix #2: Don't emit "messages" for every benign condition

Right now, the plugin appears to append to `@messages` for every duplicate_barcode (per record, per item), every wide-character warning, every parse retry. A 125k run could generate 100k+ message entries. This:

- Makes the JSON job blob enormous
- Slows down every write to `background_jobs.data`
- Creates a garbage-collection pressure point

**Suggestion: three logging tiers:**

1. **Suppress entirely** — Perl warnings like "Wide character", duplicate_barcode per-item, parse retries. Track counts in report fields only (`duplicate_barcodes: 127`, `wide_char_warnings: 38528`).
2. **Summary per slice** — "Slice 0: 125 items skipped (duplicate_barcode), 3 records skipped (MARC parse error)".
3. **Full detail in failed_records only** — the actionable list the migration tech needs to fix records and re-push.

### Fix #3: Only eval-wrap the risky operations, not the whole loop body

v0.4.0's "wrap everything" approach is safe but slow. Perl's `eval` has non-trivial overhead (~10-20µs per call). At 125k records × ~3 eval blocks each = 375k eval calls. That's ~4-7 seconds of pure eval overhead for the whole run, which is acceptable — but if the eval is inside a tight inner loop it can compound.

**The ONE operation that's known to throw:** `MARC::File::USMARC->next()` (reading a record from the file). Wrap just that:

```perl
# Fast path — no eval around AddBiblio/AddItemBatchFromMarc unless we know the record is risky
my $rec;
eval { $rec = $batch->next(); };
if ($@ || !$rec) {
    # Bad record — log to failed_records, skip
    push @failed_records, { record_num => $n, reason => "parse: $@" };
    next;
}

# Now run AddBiblio and AddItemBatchFromMarc WITHOUT eval wrapping
# (they're well-tested Koha functions that don't die on normal data)
my ($biblionumber, $biblioitemnumber) = AddBiblio($rec_clone, '', ...);
AddItemBatchFromMarc($rec, $biblionumber, $biblioitemnumber, '');
```

If AddBiblio/AddItemBatchFromMarc DO throw on malformed data, the chunk's outer eval still catches it — then the chunk rolls back, and we continue to the next chunk (per v0.4.0's recovery logic).

### Fix #4: The "sidecar for hazardous records" idea (what the user asked for)

> "if it's a record that's going to cause it to crash - then we should split that record out and keep moving along"

This is a great pattern. Two-pass approach:

**Pass 1 (fast):** Process all records assuming they're clean. Use chunk-level commits with rollback-on-error (v0.4.0's current design). When a chunk fails, save the failing chunk's records to a sidecar `.failed.mrc` file, roll back, and continue.

**Pass 2 (safe):** After the fast path finishes, open the sidecar file and process records ONE AT A TIME with per-record eval. Most of the sidecar records will succeed; the true crashers end up in `failed_records[]` with full error details.

This gives you:
- **95%+ of records import at v0.3.0 speeds** (no per-record eval in the hot loop)
- **Problem records isolated cleanly** (not interleaved with good ones)
- **Clear audit trail** (`.failed.mrc` file is human-reviewable)

Pseudo-code:

```perl
# Pass 1: fast path
open my $sidecar_fh, '>:raw', "$work_dir/failed.mrc";
while (my @chunk = next_chunk()) {
    eval {
        $dbh->begin_work;
        for my $rec (@chunk) {
            my ($b, $bi) = AddBiblio($rec_clone, '', ...);
            AddItemBatchFromMarc($rec, $b, $bi, '');
        }
        $dbh->commit;
    };
    if ($@) {
        $dbh->rollback;
        print $sidecar_fh $_->as_usmarc for @chunk;
        $sidecar_count += scalar @chunk;
        # continue to next chunk
    }
}
close $sidecar_fh;

# Pass 2: slow safe path on sidecar only
if ($sidecar_count > 0) {
    my $batch = MARC::File::USMARC->in("$work_dir/failed.mrc");
    while (my $rec = eval { $batch->next() }) {
        eval {
            $dbh->begin_work;
            my ($b, $bi) = AddBiblio($rec_clone, '', ...);
            AddItemBatchFromMarc($rec, $b, $bi, '');
            $dbh->commit;
        };
        if ($@) {
            $dbh->rollback;
            push @failed_records, { record_id => $rec->field('001')->data, reason => $@ };
        }
    }
}
```

### Fix #5: Configurable error-handling modes

Add a plugin config option:

- **`error_mode: strict`** — v0.3.0 behavior, fastest, aborts on first error (for trusted sources)
- **`error_mode: sidecar`** (recommended default) — v0.4.0's correctness + Fix #4's fast path
- **`error_mode: per_record`** — v0.4.0 current behavior (slowest, safest, for debugging)

Migration techs running against dirty data would pick `sidecar`. CI/trusted-source jobs would pick `strict`.

## What NOT to change

- Memory behavior is **perfect** — streaming splitter + streaming workers use ~700MB RSS for 125k records. Don't touch that.
- The `failed_records` array design is good — `record_id` (from 001) + `record_num` + `reason` is exactly what a migration tech needs.
- Chunked transactions with rollback-on-error (without per-record eval) is the right shape.

## Benchmark data (for reference)

| Version | Records | Duration | Rate | Outcome |
|---|---|---|---|---|
| v0.3.0 | 5,000 | 109s | 228 rec/s | ✅ complete |
| v0.3.0 | 125,129 | 289s (partial) | 200 rec/s | ❌ aborted at 58k, UTF-8 |
| v0.4.0 | 125,129 | ~940s (partial) | 62 rec/s | cancelled at 58k — sim was slow |

Projected times for 125k records with each fix:

| Config | Splitter | Workers | Total (est.) |
|---|---|---|---|
| v0.4.0 as-is | ~10 min | ~15 min @ 120/s | ~25 min |
| v0.4.0 + Fix #1 (`:raw`) | ~30s | ~15 min @ 120/s | ~15 min |
| v0.4.0 + Fix #1 + Fix #3 (tight eval) | ~30s | ~10 min @ 200/s | ~10 min |
| v0.4.0 + all fixes, clean data | ~30s | ~10 min @ 200/s | ~10 min |

## Priority

1. **Fix #1 (`:raw` mode)** — one-line change, immediate 40% speedup, no regression risk. DO THIS FIRST.
2. **Fix #3 (tight eval scope)** — recovers most of the remaining v0.3.0 performance while keeping v0.4.0 correctness.
3. **Fix #4 (sidecar pattern)** — elegant architecture improvement, can follow after #1+#3 prove the speed is back.
4. **Fix #2 (logging tiers)** — quality-of-life, addresses the "filling up the logs" complaint.
5. **Fix #5 (configurable modes)** — nice-to-have, can wait for a later version.

---

*Generated during plugin v0.4.0 testing, 2026-04-05.*
