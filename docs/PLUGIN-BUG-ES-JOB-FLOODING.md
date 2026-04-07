# Plugin Bug: 598k ES Index Jobs Queued During Fast Import

**Plugin:** Koha::Plugin::BWS::MigrationToolkit v1.0
**Severity:** High — causes massive background job queue buildup
**Found during:** Akron migration (470k bibs + 598k items)

## The Problem

After a fast import of 470,000 records, we found **598,301 `update_elastic_index` background jobs** queued in Koha's `background_jobs` table. These are per-record ES index jobs that Koha's internal code creates automatically when `AddBiblio()` and `AddItemBatchFromMarc()` are called.

The plugin correctly:
- Stops the ES indexer daemon (`koha-es-indexer --stop`)
- Reports `daemons_stopped: 1`
- Defers indexing to TurboIndex

But the daemon stop only prevents **processing** of ES jobs — it does NOT prevent Koha from **creating** them. Every `AddBiblio` and `AddItemBatchFromMarc` call still queues an `update_elastic_index` job via Koha's internal hooks.

## Impact

- 598k rows inserted into `background_jobs` table during import — massive DB bloat
- If the ES indexer daemon gets restarted (or wasn't fully stopped), it tries to process all 598k jobs sequentially — competing with TurboIndex and taking hours
- The `background_jobs` table becomes unusable in the Koha staff UI (shows "598,301 entries")
- We had to manually `DELETE FROM background_jobs WHERE type='update_elastic_index'` to clean up

## Root Cause

### AddBiblio path
The plugin passes `skip_record_index => 1` to `AddBiblio()`:
```perl
my ($biblionumber, $biblioitemnumber) = AddBiblio($rec_clone, '', {
    skip_record_index => 1,
    disable_autolink  => 1,
});
```
This correctly prevents `AddBiblio` from queueing an ES job. ✅

### AddItemBatchFromMarc path
```perl
AddItemBatchFromMarc($original_record, $biblionumber, $biblioitemnumber, '');
```
**This does NOT pass `skip_record_index`.** The `AddItemBatchFromMarc` function (in `C4::Items`) calls `AddItem` for each 952 field, and `AddItem` calls `Koha::Item->store()`, which triggers `_after_item_action_hooks` → `index_records()` → queues an `update_elastic_index` background job.

## The Fix

### Option A: Pass skip_record_index to AddItemBatchFromMarc (preferred)

Check if `AddItemBatchFromMarc` accepts a `skip_record_index` option. In recent Koha versions (23.11+), `Koha::Item->store()` respects `$self->{_skip_record_index}`. The plugin can set this:

```perl
# Before calling AddItemBatchFromMarc, set the env var that suppresses indexing
local $ENV{OVERRIDE_SYSPREF_SearchEngine} = 'Zebra';  # hack: makes ES indexer think it's Zebra
# OR
# Pass the flag if AddItemBatchFromMarc supports it:
AddItemBatchFromMarc($original_record, $biblionumber, $biblioitemnumber, '', {
    skip_record_index => 1,
});
```

### Option B: Suppress via environment variable

The cleanest approach used by `bulkmarcimport.pl` itself:

```perl
# In the worker process, before the import loop:
$ENV{KOHA_SKIP_RECORD_INDEX} = 1;
# This is checked by Koha::SearchEngine::Indexer
```

Check `Koha::SearchEngine::Indexer::index_records()` to see if it respects this env var. If not, the plugin can monkey-patch it:

```perl
# Temporarily replace index_records with a no-op during import
no warnings 'redefine';
local *Koha::SearchEngine::Indexer::index_records = sub { return; };
```

### Option C: Delete queued jobs after import

Least elegant but guaranteed to work:

```perl
# After import completes, before restarting daemons:
my $schema = Koha::Database->new->schema;
$schema->resultset('BackgroundJob')->search({
    type   => 'update_elastic_index',
    status => 'new',
})->delete;
```

Add a message to the report: `"Cleaned up N queued ES index jobs"`.

### Option D: Disable the ES indexer syspref during import

```perl
# Before import
C4::Context->set_preference('SearchEngine', 'Zebra');
# or
C4::Context->set_preference('ElasticsearchIndexStatus_biblios', 'recreate_required');

# After import
C4::Context->set_preference('SearchEngine', 'Elasticsearch');
```

This is dangerous because it changes a system preference that affects all users. Only use this if the Koha instance is in maintenance mode.

## Recommended Fix

**Option A + Option C combined:**

1. Pass `skip_record_index => 1` through to the item creation path (prevents new jobs from being queued)
2. As a safety net, clean up any queued `update_elastic_index` jobs after import completes (catches edge cases)
3. Report the cleanup count in the job report: `"es_jobs_cleaned": 598301`

## How to Verify the Fix

After a test import of N records:
```sql
SELECT COUNT(*) FROM background_jobs 
WHERE type = 'update_elastic_index' AND status = 'new';
```
Should be **0** (or very close to 0). If it's anywhere near N, the fix didn't work.

## Reference: How bulkmarcimport.pl Handles This

Koha's own `bulkmarcimport.pl` (the command-line import tool) avoids this problem by:

1. Setting `skip_record_index => 1` on every `AddBiblio` call
2. NOT calling `AddItemBatchFromMarc` at all — it processes items separately with direct `Koha::Item->new(...)->store()` calls that have `skip_record_index` set
3. Doing a single `index_records()` call at the end for all biblionumbers

The plugin's `AddItemBatchFromMarc` approach is simpler but hits this ES job queueing side effect. Either switch to the `bulkmarcimport.pl` pattern for items, or find a way to pass the skip flag through `AddItemBatchFromMarc`.

---

*Found during Cerulean Next integration testing, April 2026.*
*Akron migration: 470k bibs, 598k items, 598,301 spurious ES jobs.*
