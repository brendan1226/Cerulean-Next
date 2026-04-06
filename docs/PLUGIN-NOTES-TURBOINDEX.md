# TurboIndex Plugin — Notes for Future Development

Notes from Cerulean Next integration testing (v0.1.0).
Captures bugs, architectural learnings, and recommendations for any
migration platform integrating with this plugin.

## Critical Bug: `resource_already_exists_exception` on `reset: true`

**Observed in every test run.** When `reset: true`, the plugin tries to
drop and recreate the ES index. It fails with:

```
[400] [resource_already_exists_exception] index [koha_kohadev_biblios/...]
already exists
```

**Root cause:** Koha's ES indexer daemon (`es_indexer_daemon.pl`) re-creates
the index within milliseconds of deletion. The plugin's `DELETE` + `PUT`
has a race condition.

**Fix options:**

1. **Stop the ES indexer daemon before reset** — the migration mode
   approach. Plugin could do this itself if given a `migration_mode`
   parameter.

2. **Use `PUT /{index}` with `"settings": {"index": {...}}` on the
   existing index** — skip delete entirely, just update mappings and
   settings in place, then reindex into it.

3. **Use ES's close/reopen pattern:**
   ```
   POST /{index}/_close
   PUT /{index}/_settings  (update refresh_interval, replicas)
   POST /{index}/_open
   ```

4. **Defensive retry with delay:**
   ```perl
   for my $attempt (1..3) {
       eval { $es->indices->delete(index => $index_name) };
       sleep(1);  # wait for es_indexer to settle
       eval { $es->indices->create(index => $index_name, ...) };
       last unless $@;
   }
   ```

**Our workaround:** Cerulean's migration mode stops the ES indexer
before dispatching TurboIndex. This reliably avoids the race.

## Critical Bug: `NoNodes` in forked workers (v0.1.0, partially fixed)

**Observed:** All forked worker processes get `NoNodes` error on every
ES bulk call when run without migration mode.

**Root cause:** The parent process's `Search::Elasticsearch` client
connection handle gets inherited by fork children but the socket/DNS
resolution is stale.

**Fix:** Each child must create a fresh ES client after fork. Similar to
how FastMARCImport does `Koha::Database->schema->storage->disconnect`
before forking for DB handles.

```perl
# Before fork
$es_client = undef;  # force re-creation in child

# In child
my $es = Koha::SearchEngine::Elasticsearch->get_elasticsearch_client();
```

**Status as of v0.1.0:** Partially fixed. In our tests, one worker
succeeded (45k records indexed) while the other got `NoNodes` (17k
errors). With migration mode (ES indexer stopped), both workers succeed.

## Migration Mode Integration

### The ideal flow

```
1. Stop ES indexer daemon + Zebra daemon
2. FastMARCImport (bibs + items, skip_record_index=1)
3. TurboIndex (reset=true, processes=4, commit=5000)
4. Restart ES indexer daemon + Zebra daemon
```

### Plugin-side recommendation

Accept `migration_mode: true` in the request body. When set:

```perl
sub process {
    my ($self, $args) = @_;
    
    if ($args->{migration_mode}) {
        # Stop ES indexer to prevent race conditions
        system("koha-es-indexer --stop $instance");
    }
    
    # ... do the reindex ...
    
    if ($args->{migration_mode}) {
        # Restart ES indexer
        system("koha-es-indexer --start $instance");
    }
}
```

This makes the plugin self-contained for migration scenarios.

## ES Bulk Mode Settings

The plugin correctly:
- Sets `refresh_interval: -1` and `number_of_replicas: 0` before indexing
- Restores original values after indexing
- Reports `saved_settings` in the response

**Observation:** when the ES indexer daemon is running, it may reset these
settings between the plugin's `PUT /_settings` and the actual bulk
indexing, negating the optimization. Another reason to stop the daemon.

## Performance Data

| Records | Processes | Duration | Rate | Status |
|---|---|---|---|---|
| 125k | 4 | — | — | Failed (NoNodes in all workers, ES daemon running) |
| 125k | 2 | 377s | 119 rec/s | Partial (45k indexed, 17.5k errors, ES crashed) |
| 125k | 4 | ~10 min (projected) | ~200 rec/s | With migration mode, upsized RAM |

**Scaling note:** ES bulk indexing is mostly IO-bound (writing Lucene
segments). More than 4 processes may not help on a single ES node. The
real bottleneck is the MARC→ES document serialization in Perl
(`Koha::SearchEngine::Elasticsearch::Indexer::update_index`).

## Report Fields

Current fields returned in `data.report`:
```
num_indexed, num_errors, duration_sec, records_per_sec,
processes, reset, force_merge, saved_settings
```

**Suggested additions:**
- `records_per_worker` — array of per-process counts (validates load
  balancing)
- `es_bulk_time_sec` — time spent in actual ES bulk calls (vs MARC
  serialization)
- `serialization_time_sec` — time converting MARC to ES documents
- `force_merge_time_sec` — time for the final segment merge

## Feature Requests

1. **`migration_mode` parameter** — stop/start ES indexer around the
   reindex (see above).
2. **Per-worker progress** — report which workers are active and their
   individual throughput.
3. **Dry-run mode** — validate MARC→ES serialization without writing,
   to catch records that will fail before committing to a full reindex.
4. **Incremental mode** — `reset: false` should update existing docs
   rather than insert. Useful for re-indexing a subset of records after
   a patch.
5. **Configurable ES endpoint** — currently reads from `koha-conf.xml`.
   A request-body override would help testing against a different ES
   cluster.

---

*From Cerulean Next integration testing, April 2026.*
