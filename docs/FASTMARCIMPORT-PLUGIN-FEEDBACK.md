# Fast MARC Import Plugin — Feedback & Recommendations

**Context:** Testing v0.3.0 of the `koha-plugin-fastmarcimport` against a 125k-record
Koha migration file via Cerulean Next integration. This document captures findings
from three test runs and suggests plugin improvements.

## Test environment

- **Koha:** kohadev (kohadev-koha-1), 2 plugin workers, txn_chunk=1000
- **Cerulean:** pushes via `POST /api/v1/contrib/fastmarcimport/import` with streaming upload
- **MARC file:** 125,129 records, 165,299 items (952 fields), 268 MB, declared Leader[9]='a' (UTF-8)
- **Source data:** real library MARC with French/Portuguese diacritics (é, í, ç, etc.) in ~38,546 records

## Summary: what works brilliantly

- **Memory fix in v0.3.0 (streaming splitter + workers):** Confirmed working. Peak RSS held at ~700MB for 125k records (v0.2.0 OOMed at ~3.8GB).
- **Throughput:** 200+ records/sec with 2 workers on the partial runs (before bailout).
- **Single-call flow:** Cerulean's integration is clean — one POST, one job to poll, no separate commit step. Dramatically simpler than `/biblios/import` + `/import_batches/{id}/commit`.
- **Syspref overrides:** Big win. Deferring `CataloguingLog`, `AutoLinkBiblios`, `RealTimeHoldsQueue`, and `OAI-PMH:AutoUpdateSets` eliminates per-record side effects during bulk load.
- **Batched indexing at the end:** `Indexing 58246 records in a single batch call` is exactly right — much faster than per-record reindex.
- **Error reporting in the job blob:** `num_added`, `num_errors`, `num_items_added`, `num_item_errors`, `records_per_sec`, `worker_count`, `duration_sec`, `syspref_overrides` — this is everything Cerulean needs.

## Critical bug: UTF-8 errors abort an entire slice

**Observed behavior (3 independent runs):**

```
[warning] Slice 0 rec 16 item: duplicate_barcode
[error] Slice 0 chunk failed, rolled back current chunk:
        UTF-8 "\xA9" does not map to Unicode at /usr/share/perl5/MARC/File/Encode.pm line 35.

[warning] Slice 1 rec 13 item: duplicate_barcode
[error] Slice 1 chunk failed, rolled back current chunk:
        UTF-8 "\xA9" does not map to Unicode at /usr/share/perl5/MARC/File/Encode.pm line 35.

[info] All workers finished. Added=58246 Errors=1246 ...
```

**Impact:** Both workers stopped at the same point each run (~29k records each = 58k total). Of 125k input records, only 46% imported before the UTF-8 error halted processing. The `chunk_failed_rolled_back` log line suggests the plugin **rolled back the current chunk** but then **stopped processing further chunks in that slice**.

**Root cause:** Somewhere in Perl's call stack (`MARC::File::Encode` line 35), a byte range is being decoded that ends mid-codepoint. This appears to happen on records containing Latin-1 Supplement multi-byte UTF-8 sequences (`\xc3\xa9` = é, `\xc3\xad` = í, etc.) where MARC byte-length directory entries interact badly with Perl's strict UTF-8 decode path.

**We verified pymarc reads all 125k records cleanly with `utf8_handling='strict'`.** The bytes are not actually malformed UTF-8 at the file level. This is a Perl-specific edge case.

### Recommended fix

The current chunk handler looks something like:

```perl
eval {
    $dbh->begin_work;
    for my $rec (@chunk_records) {
        my ($biblionumber, $biblioitemnumber) = AddBiblio($rec_clone, '', ...);
        AddItemBatchFromMarc($rec, $biblionumber, $biblioitemnumber, '');
    }
    $dbh->commit;
};
if ($@) {
    $dbh->rollback;
    # BUG: slice stops here?
}
```

**Fix #1 — Per-record try/catch inside the chunk loop:**

```perl
$dbh->begin_work;
for my $rec (@chunk_records) {
    eval {
        my ($biblionumber, $biblioitemnumber) = AddBiblio($rec_clone, '', ...);
        AddItemBatchFromMarc($rec, $biblionumber, $biblioitemnumber, '');
    };
    if ($@) {
        $num_errors++;
        push @messages, { type => 'warning',
                          message => "Slice $slice_id rec $rec_num: $@" };
        # continue to next record — don't abort the chunk
    }
}
$dbh->commit;
```

This keeps the transaction benefits (batched commits) but never loses a whole chunk of records to one bad row.

**Fix #2 — Continue after rollback:**

If a chunk DOES have to roll back (e.g. real DB-level failure, not a parse error), the worker should log and continue to the **next** chunk, not stop.

```perl
# chunk loop
while (my @chunk = next_chunk()) {
    eval {
        process_chunk(\@chunk);
    };
    if ($@) {
        # chunk rolled back — log and move on
        push @messages, { type => 'error', message => "Slice $slice_id chunk: $@" };
        # DO NOT return / last / die here
    }
}
```

**Fix #3 — Stop using strict UTF-8 decode where possible:**

The `MARC::File::Encode` line 35 failure is triggered by strict UTF-8 decoding on a byte range that isn't a valid UTF-8 boundary. Koha's own `AddItemBatchFromMarc` path may be calling a method that defaults to strict. Consider patching to use `Encode::decode('UTF-8', $bytes, Encode::FB_DEFAULT)` (replacement char) instead of strict mode, matching how `bulkmarcimport.pl` handles the same data.

## Secondary observations

### Item `duplicate_barcode` warnings

If the source file is pushed multiple times (common during testing/migration dry-runs), item barcodes from prior runs cause `duplicate_barcode` warnings that flood the message log and inflate `num_item_errors`. These are individually benign (items get skipped, biblios still commit), but:

1. The messages accumulate to hundreds/thousands per run, making the `messages` array huge.
2. Users can't tell real errors from "already imported" noise.

**Suggestion:** Collapse duplicate_barcode warnings into a single summary message per slice: `"Slice 0: 25 items skipped (duplicate_barcode)"` rather than logging each one.

### Progress reporting could be more granular

`background_jobs.progress` updates roughly every N records (looks like ~100). For a long job that's fine, but for a user watching a progress bar it can feel "stuck" for seconds between updates. This is a minor polish item.

### Stop the whole job on non-recoverable errors only

Current behavior seems to be: any chunk failure → slice stops. Better:
- DB-level errors (lost connection, constraint violations on parent tables): stop the slice, the whole job likely can't recover
- Record-level errors (malformed data, duplicate keys, decode issues): log, skip, continue

## Plugin report fields — suggested additions

Currently the `report` hash includes:
```
num_added, num_errors, num_items_added, num_item_errors,
duration_sec, records_per_sec, worker_count, syspref_overrides
```

Adding these would help post-run analysis:
- `records_processed` — records actually attempted (vs `num_added` which is only successes)
- `slices_completed` — how many slices ran to completion vs aborted
- `first_error_message` — the first non-warning error, surfaced separately from the messages array
- `chunks_rolled_back` — count of chunks that rolled back (distinguishes parse errors from DB errors)

## What Cerulean ships today (for reference)

Cerulean's push task currently:

1. Detects the plugin via `POST /api/v1/contrib/fastmarcimport/import` with empty body (expects 400 "empty body" = installed, 404 = not installed).
2. Streams the MARC file to the plugin endpoint in 1 MB chunks (`Content-Type: application/marc`, explicit `Content-Length` header).
3. Polls `/api/v1/jobs/{id}` with adaptive backoff (3s → 10s → 30s as elapsed time grows).
4. Persists `job_id`, per-file state (`pending → running → committed`) to the PushManifest row on every transition, for crash recovery.
5. Parses the plugin's report and displays: bibs added/errors, items added/errors, duration, rate, worker count.

We also ship a **UTF-8 Preflight** step that re-parses the MARC file with pymarc's strict UTF-8 mode AND counts non-ASCII records. But this **does not detect** the specific Perl failure because Python's UTF-8 decoder handles these bytes correctly. We surface non-ASCII record counts as a warning so users know to watch for Perl-side decode issues.

## Prioritized ask

If we had to pick ONE fix that would make v0.4.0 production-ready:

> **Per-record try/catch inside the chunk loop + continue-after-rollback.**

That single change would have turned our 46% partial result into a successful 100% run (with the handful of UTF-8-unhappy records logged as warnings and skipped).

---

*Generated during plugin v0.3.0 testing, 2026-04-05.*
