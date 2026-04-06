# FastMARCImport Plugin — Notes for Future Development

Notes from Cerulean Next integration testing (v0.3.0 through v0.6.0).
These capture bottlenecks, architectural learnings, and feature requests
that would benefit any migration platform integrating with this plugin.

## Migration Mode (daemon orchestration)

### The problem

Koha runs several background daemons that compete for CPU, RAM, and ES
resources during bulk imports:

| Daemon | Resource impact | Why it interferes |
|---|---|---|
| `koha-es-indexer` (es_indexer_daemon.pl) | **58% CPU**, 230MB RSS | Incrementally indexes every DB change to ES. Competes with TurboIndex and re-creates the ES index after deletion. |
| `koha-indexer` (rebuild_zebra.pl -daemon) | **34% CPU**, 260MB RSS | Zebra indexer runs every 5s. Not needed if using ES. |
| `koha-zebra` (zebrasrv) | Low CPU, 11MB RSS | Zebra search server. Not needed during migration. |
| `koha-sip` (SIPServer.pm) | Low CPU, 210MB RSS | SIP2 circulation. Not needed during migration. |
| `koha-z3950-responder` (z3950_responder.pl) | Low CPU, 185MB RSS | Z39.50 search. Not needed during migration. |

**Total savings from stopping all 5: ~92% CPU, ~900MB RAM** freed for
the import workers and ES bulk indexing.

### What Cerulean does today (v1.0)

Cerulean now has a "Migration Mode" toggle that stops/starts these
daemons via Docker exec before/after the import+reindex cycle.

### Plugin-side recommendation

The plugin itself could optionally suppress these daemons as part of its
job lifecycle, so ANY caller (not just Cerulean) gets the benefit:

```perl
# In FastImport.pm process():
sub process {
    my ($self, $args) = @_;
    
    # Before import: stop competing daemons
    my @stopped = _stop_migration_daemons() if $args->{migration_mode};
    
    # ... do the import ...
    
    # After import: restart them
    _start_migration_daemons(@stopped) if $args->{migration_mode};
}
```

Pass `migration_mode: true` as a request parameter. Default: false
(non-breaking). This way any REST client — Cerulean, curl, a custom
script — gets the same optimization without needing Docker socket access.

## Encoding Observations

### Wide character warnings (fixed in v0.5.0)

The splitter's `print $fh $serialized` emitted Perl "Wide character in
print" warnings for every non-ASCII MARC record (~38k warnings for our
125k file). Fixed by `binmode($fh, ':raw')` + `$SIG{__WARN__}` filter.

**Remaining issue:** the splitter phase still emits ~38k warnings to the
log file (line 392 in v0.6.0) even though workers suppress them. The
`$serialized` variable from `$record->as_usmarc` returns a Perl
character string, not bytes. Fix:

```perl
use Encode qw(encode_utf8 is_utf8);
my $serialized = $record->as_usmarc;
$serialized = encode_utf8($serialized) if is_utf8($serialized);
print { $out_fhs[$target] } $serialized;
```

### MARC::File::Encode failures

18 out of 125,129 records fail Perl's MARC decode with errors like:
```
UTF-8 "\xA9" does not map to Unicode at MARC/File/Encode.pm line 35
```

These records contain valid UTF-8 (verified by Python's strict decoder)
but Perl's `MARC::File::USMARC->next()` chokes on them during the
**splitter's record-read phase**. The sidecar architecture correctly
skips them and reports them in `failed_records[]`.

**Root cause hypothesis:** `MARC::File::Encode` uses strict UTF-8 decode
on byte ranges calculated from the MARC directory. When a field contains
multi-byte UTF-8 chars (e.g., `\xc3\xa9` = `e`), the byte count in the
directory is correct, but Perl's decode reads a character count's worth
of bytes, overshooting and landing mid-codepoint.

**Possible fix:** use `Encode::FB_DEFAULT` (replacement character) instead
of strict mode in `MARC::File::Encode`, or apply the same tolerance that
`bulkmarcimport.pl` uses.

## Performance Benchmarks

| Workers | Records | Duration | Throughput | Notes |
|---|---|---|---|---|
| 2 | 5,000 | 13s | 385 rec/s | v0.3.0, bibs only |
| 2 | 125,129 | 1189s | 105 rec/s | v0.6.0, bibs + items |
| 4 | 125,129 | 774s | 162 rec/s | v0.6.0, bibs + items, upsized host |

**Scaling:** 4 workers was 53% faster than 2 workers on the same data.
Diminishing returns expected beyond 4 due to DB lock contention on
`biblio`/`items` tables.

## Report field accuracy (v0.6.0)

- `num_items_added` sometimes reports 0 when items ARE successfully
  created (verified ~165k items in DB). The counter may not be
  incrementing in `AddItemBatchFromMarc`'s success path.
- `duplicate_barcodes` reported 165k on a run where no duplicates
  existed (fresh DB). May be inverting the success/failure check.

## Feature requests for future versions

1. **`migration_mode` request parameter** — stop/start competing daemons
   around the import (see above).
2. **`worker_count` as request parameter** — currently admin-config-only.
   Would let callers tune per-push without changing plugin settings.
3. **Runtime-configurable `txn_chunk_size`** — same as above.
4. **Progress during splitter phase** — currently progress stays at 0/0
   until workers start. Emitting progress during the file-split would
   help large files (~45s of apparent "stall" on 125k records).
5. **Partial-completion report** — when some slices succeed and others
   fail, the report should clearly show per-slice outcomes.

---

*From Cerulean Next integration testing, April 2026.*
