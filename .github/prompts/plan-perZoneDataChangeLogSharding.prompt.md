## Plan: Per-Zone Data Change Log Sharding

The current `RGWDataChangesLog` writes all bucket changes to a single set of 128 shards (`data_log.{0..127}`). We will introduce per-zone log instances so each target zone (zones syncing data from us) gets its own set of 128 shards, with OIDs like `data_log.{zone_id}.{shard}`. The legacy log is preserved for backward compatibility with older peers. A new `RGWDataChangesLogManager` container class holds both the legacy log and a map of per-zone logs, exposing parallel fan-out writes and zone-aware list/trim.

Each datalog ingest requires 2 RADOS writes per log instance (prepare via semaphore increment, commit via `be->push()`). With N target zones, the manager fans out N+1 log writes (legacy + per-zone) **in parallel** using `spawn_group`, keeping latency constant regardless of the number of destinations. Destinations include local-zonegroup target zones and, for master zones in a master-to-master topology, zones in remote zonegroups that have cross-zonegroup replication policy. Non-master zones only write datalogs for their own zonegroup's targets.

**Steps**

1. **Make `RGWDataChangesLog` prefix configurable**
   - In [src/rgw/driver/rados/rgw_datalog.h](src/rgw/driver/rados/rgw_datalog.h), the `prefix` member is initialized from `get_prefix()` which always returns `"data_log"`. Add an optional `std::string prefix` parameter to both constructors (production & test) so callers can pass e.g. `"data_log.{zone_id}"`. Default to `get_prefix()` when omitted.
   - In [src/rgw/driver/rados/rgw_datalog.cc](src/rgw/driver/rados/rgw_datalog.cc) constructors (~lines 361-378), accept and store the custom prefix. This affects `get_oid()`, `metadata_log_oid()`, `get_sem_set_oid()` — all of which already derive from `prefix`, so no further changes needed in those methods.

2. **Create `RGWDataChangesLogManager` container class**
   - New class in [src/rgw/driver/rados/rgw_datalog.h](src/rgw/driver/rados/rgw_datalog.h) and [src/rgw/driver/rados/rgw_datalog.cc](src/rgw/driver/rados/rgw_datalog.cc).
   - Members:
     - `unique_ptr<RGWDataChangesLog> legacy_log` — the existing single log (prefix `"data_log"`)
     - `map<rgw_zone_id, unique_ptr<RGWDataChangesLog>> zone_logs` — per-zone logs (prefix `"data_log.{zone_id}"`)
     - `rgw::sal::RadosStore* driver` — for constructing per-zone log instances
     - `bool is_master_zone` — whether this zone is a master zone (controls cross-zonegroup log writes)
   - Methods:
     - `init(dpp, driver, zone, zone_params, local_targets, cross_zonegroup_targets, background_tasks)` — creates and starts the legacy log; creates per-zone logs for local zonegroup targets, and if `is_master_zone`, also for cross-zonegroup replication destinations
     - `add_entry(dpp, bucket_info, gen, shard_id)` — **`asio::awaitable<void>`** that fans out writes in parallel:
       ```cpp
       spawn_group group{ex, zone_logs.size() + 1};
       co_spawn(ex, legacy_log->add_entry(dpp, ...), group);
       for (auto& [zid, zlog] : zone_logs)
           co_spawn(ex, zlog->add_entry(dpp, ...), group);
       co_await group.wait();
       ```
       Uses `spawn_group` (already used in [rgw_datalog.cc#L1640](src/rgw/driver/rados/rgw_datalog.cc#L1640) for parallel recovery) to keep latency constant. Also provide `yield_context` and `optional_yield` overloads that mirror `RGWDataChangesLog`'s existing interface.
     - `get_legacy_log() -> RGWDataChangesLog*`
     - `get_zone_log(zone_id) -> RGWDataChangesLog*` — returns `nullptr` if zone not found
     - `get_zone_ids() -> vector<rgw_zone_id>`
     - `list_entries(zone_id, shard, ...)` — delegates to zone log (or legacy if no zone_id)
     - `trim_entries(zone_id, shard, marker)` — delegates to zone log
     - `get_info(zone_id, shard)` — delegates to zone log
     - `read_clear_modified_by_zone()` — returns per-zone modified shards (see step 6)
     - `set_observer(observer)` — forwards to all logs
     - `set_bucket_filter(filter)` — forwards to all logs
     - Shutdown: `stop()` calls `legacy_log->stop()` and all `zone_logs[*]->stop()`

3. **Add per-zone `modified_shards` tracking**
   - Currently `mark_modified()` in `RGWDataChangesLog` populates `modified_shards` as a flat shard→entries map. Each per-zone `RGWDataChangesLog` instance will naturally track its own `modified_shards` since each is a separate object.
   - `RGWDataChangesLogManager::read_clear_modified_by_zone()` returns `map<rgw_zone_id, flat_map<int, flat_set<rgw_data_notify_entry>>>` by calling `read_clear_modified()` on each zone log.

4. **Wire up initialization in `RGWServices_Def`**
   - In [src/rgw/driver/rados/rgw_service.h](src/rgw/driver/rados/rgw_service.h), add `unique_ptr<RGWDataChangesLogManager> datalog_manager` to `RGWServices_Def` (alongside the existing `datalog_rados` for now).
   - In [src/rgw/driver/rados/rgw_service.cc](src/rgw/driver/rados/rgw_service.cc) `init()` (~line 65), construct `datalog_manager`. After `zone` service is initialized (~line 136):
     - Get local zonegroup targets from `zone->get_zone_data_notify_to_map()`
     - If this is a master zone (`zonegroup->master_zone == zone->id`), also resolve cross-zonegroup replication destinations from sync policy (zones in other zonegroups that `target_zones` references). Currently, `svc_zone.cc` only builds REST connections for intra-zonegroup zones (~line 185); for cross-zonegroup targets, connections would come from `zonegroup_conn_map` (master zones of other zonegroups, ~line 350) or new connections created for non-master zones in remote zonegroups.
     - Call `datalog_manager->init(...)` with both sets of targets.
   - Keep `datalog_rados` pointing to `datalog_manager->get_legacy_log()` so **all existing consumers continue to work unchanged** during the transition.
   - In `RGWServices` (~line 133), add `RGWDataChangesLogManager* datalog_manager{nullptr}` raw pointer and set it at line 283.

5. **Update `add_datalog_entry()` to fan-out writes**
   - In [src/rgw/driver/rados/rgw_rados.cc](src/rgw/driver/rados/rgw_rados.cc) `add_datalog_entry()` (~line 782), change to call through `datalog_manager->add_entry(...)` instead of `datalog->add_entry(...)`. This single change covers **all** callers since they all go through this helper.
   - Similarly update the direct `add_entry` calls in [src/rgw/services/svc_bi_rados.cc](src/rgw/services/svc_bi_rados.cc) (~line 1038), [src/rgw/driver/rados/rgw_bucket.cc](src/rgw/driver/rados/rgw_bucket.cc) (~line 3037), and [src/rgw/driver/rados/rgw_reshard.cc](src/rgw/driver/rados/rgw_reshard.cc) (~line 884) to go through the manager.

6. **Update `RGWDataNotifier` for per-zone notifications**
   - In [src/rgw/driver/rados/rgw_rados.cc](src/rgw/driver/rados/rgw_rados.cc) `RGWDataNotifier::process()` (~line 454), instead of reading one set of modified shards and sending to all zones, call `datalog_manager->read_clear_modified_by_zone()` and for each zone, send only that zone's modified entries to that zone's REST connection via `notify_mgr.notify_all()`.

7. **Update REST log handlers for zone-aware access**
   - In [src/rgw/driver/rados/rgw_rest_log.cc](src/rgw/driver/rados/rgw_rest_log.cc), update `RGWOp_DATALog_List`, `RGWOp_DATALog_Delete`, `RGWOp_DATALog_ShardInfo`, and `RGWOp_DATALog_Status`:
     - Accept an optional `zone-id` query parameter
     - If `zone-id` present: use `datalog_manager->get_zone_log(zone_id)` and delegate to that zone's log
     - If absent: fall back to `datalog_manager->get_legacy_log()` (backward compat)
   - This allows sync consumers on newer peers to fetch zone-specific logs, while older peers still read the legacy log.

8. **Update trim for independent per-zone operation**
   - In [src/rgw/driver/rados/rgw_trim_datalog.cc](src/rgw/driver/rados/rgw_trim_datalog.cc) `DataLogTrimCR` (~line 108):
     - Currently queries all peer zones, takes the minimum marker per shard, trims the single log up to that minimum.
     - Add a new path: for each zone that has a per-zone log, query only **that zone's** sync status and trim that zone's log based solely on that zone's progress.
     - The legacy log continues to be trimmed with the existing cross-zone-minimum logic for backward compat.

9. **Update sync consumers (future step, optional for initial PR)**
   - In [src/rgw/driver/rados/rgw_data_sync.cc](src/rgw/driver/rados/rgw_data_sync.cc), `RGWReadRemoteDataLogShardCR` reads from the source zone via `GET /admin/log/?type=data&id={shard}`. To use per-zone logs, it would add `&zone-id={our_zone_id}` to the request, telling the source zone to serve entries from the log specific to us.
   - This can be deferred since the legacy log ensures no data loss — zone-specific logs are an optimization.

**Verification**

- Unit tests: Extend existing datalog tests (search for `TEST_F` in `test_rgw_datalog*` or `test_datalog`) to verify:
  - Multiple `RGWDataChangesLog` instances with different prefixes write to separate RADOS objects
  - `RGWDataChangesLogManager::add_entry` writes to both legacy and per-zone logs in parallel
  - Per-zone trim doesn't affect other zones' logs
  - Parallel fan-out latency is bounded (not serial N×latency)
- Integration: In a multi-zone vstart cluster, verify:
  - `radosgw-admin datalog list --shard-id=0` still returns legacy entries
  - `radosgw-admin datalog list --shard-id=0 --zone-id=<target>` returns zone-specific entries
  - Sync works via legacy log (backward compat)
  - Trim of one zone's log doesn't affect another zone's entries
- Master-only cross-zonegroup: In a multi-zonegroup setup, verify non-master zones do not create per-zone logs for remote zonegroup targets

**Decisions**

- **Container pattern over single-class expansion**: Chose a separate `RGWDataChangesLogManager` wrapping multiple `RGWDataChangesLog` instances over adding zone-map complexity inside `RGWDataChangesLog` — keeps each log instance simple and self-contained.
- **Legacy log preserved**: The `"data_log.*"` objects are kept alongside new `"data_log.{zone_id}.*"` objects to ensure rolling upgrade safety with older peers.
- **Parallel fan-out via `spawn_group`**: Each `add_entry` call spawns parallel coroutines for all log instances (legacy + per-zone) using the existing `spawn_group` pattern from [rgw_datalog.cc#L1640](src/rgw/driver/rados/rgw_datalog.cc#L1640). This keeps ingest latency constant at ~2 RADOS writes (prepare/commit) regardless of destination count, rather than serial N×2 writes.
- **Cross-zonegroup scope gated by master role**: Non-master zones only write per-zone logs for intra-zonegroup targets. Master zones additionally write logs for cross-zonegroup replication destinations, following the master-to-master topology convention where masters are responsible for inter-zonegroup data flow.
- **Step 9 deferred**: Sync consumer changes to read zone-specific logs are optional since the legacy log provides the same data. This de-risks the initial implementation.
