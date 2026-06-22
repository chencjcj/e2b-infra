-- name: RegisterOOMSnapshotTemplate :one
-- Atomically registers an OOM-rescue snapshot as a snapshot_template.
-- Build files are already in GCS under build_id (orchestrator wrote them
-- via snapshotAndCacheSandbox + uploadSnapshotAsync); this query writes
-- the four DB tables that make the build resolvable as a SDK template:
--
--   envs                      (source = 'snapshot_template')
--   snapshot_templates        (env_id → sandbox_id, build_id, origin_node_id)
--   env_builds                (id = supplied build_id, status = success)
--   env_build_assignments     (env_id, build_id, tag)
--
-- After this query, SDK Sandbox.create("<env_id>:<tag>") starts a new
-- sandbox from the OOM-rescue snapshot.
WITH new_env AS (
    INSERT INTO "public"."envs" (id, public, created_by, team_id, updated_at, source)
    VALUES (@template_id, FALSE, NULL, @team_id, now(), 'snapshot_template')
    RETURNING id
),

new_snapshot_template AS (
    INSERT INTO "public"."snapshot_templates" (env_id, sandbox_id, origin_node_id, build_id)
    VALUES (
        (SELECT id FROM new_env),
        @sandbox_id,
        @origin_node_id,
        @build_id
    )
),

new_build AS (
    INSERT INTO "public"."env_builds" (
        id, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb,
        kernel_version, firecracker_version, envd_version,
        status, cluster_node_id, env_id, finished_at, updated_at
    ) VALUES (
        @build_id, @vcpu, @ram_mb, @free_disk_size_mb, @total_disk_size_mb,
        @kernel_version, @firecracker_version, @envd_version,
        'success', @origin_node_id, (SELECT id FROM new_env), now(), now()
    )
    RETURNING id
),

build_assignment AS (
    INSERT INTO "public"."env_build_assignments" (env_id, build_id, tag)
    VALUES (
        (SELECT id FROM new_env),
        (SELECT id FROM new_build),
        @tag
    )
    RETURNING env_id
)

SELECT env_id FROM build_assignment;
