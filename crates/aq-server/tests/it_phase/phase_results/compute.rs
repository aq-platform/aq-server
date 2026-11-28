// Actual registration/SQL owners over the existing explicitly synthetic host
// proof fixtures. This is compute reuse, not physical runtime qualification.
use aq_server::profiled_execution::registration::{self, NativeRegistration};

async fn replacement(f: &terminal::Prepared) -> aq_admission::transport::Register {
    let row: Value = sqlx::query_scalar("SELECT state FROM execution_nodes WHERE node_id=$1")
        .bind(f.c.request.identity.node_id)
        .fetch_one(&f.c.env.env.pool)
        .await
        .unwrap();
    let mut request = serde_json::from_value::<NativeRegistration>(row["native"].clone())
        .unwrap()
        .request;
    request.operation_id = Uuid::new_v4();
    request.identity.boot_id = Uuid::new_v4();
    request
}

#[tokio::test]
async fn verified_stopped_frees_node_compute_without_releasing_task_or_requiring_result() {
    let (f, report) = Box::pin(ready()).await;
    let before = invariant(&f).await;
    let auth = terminal::auth(&f).await;
    let pool = &f.c.env.env.pool;
    let request = replacement(&f).await;
    let valid: bool = sqlx::query_scalar("SELECT profiled_compute_stopped_valid($1,$2)")
        .bind(report.binding.attempt_id)
        .bind(auth.id())
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(valid);
    let mut conn = pool.acquire().await.unwrap();
    let slots = aq_machine_control::slots::view(&mut conn, auth.id()).await.unwrap();
    let agents = aq_swarm::agents::load(
        &mut conn,
        aq_swarm::agents::Query {
            nodes: &[auth.id()],
            project: None,
            runtime: None,
            summaries: false,
            now_ms: crate::common::profiled::NOW,
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(slots.occupied, 0);
    assert!(agents.agents.is_empty(), "the released slot has no agent");
    assert!(!agents.cut);
    drop(conn);
    // A direct incarnation write cannot bypass the typed Rust validator just
    // by pointing at a plausible stopped projection or actual existing rows.
    assert!(sqlx::query("UPDATE execution_nodes SET state=jsonb_set(jsonb_set(state,'{identity}', $2),'{native,request,identity}',$2) WHERE node_id=$1")
        .bind(auth.id()).bind(json!(request.identity)).execute(pool).await.is_err());
    registration::register(&f.c.env.env.state, &auth, &request)
        .await
        .unwrap();
    assert_eq!(
        before,
        invariant(&f).await,
        "all task/source/result holds survive compute reuse"
    );
    assert!(before["held"].as_bool().unwrap());
    assert_eq!(before["release"], 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM profiled_result_reports WHERE attempt_id=$1"
        )
        .bind(report.binding.attempt_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    // The stop is historical: a later current incarnation must not invalidate
    // its exact original registration/phase/terminal provenance.
    let next = replacement(&f).await;
    registration::register(&f.c.env.env.state, &auth, &next)
        .await
        .unwrap();
    assert_eq!(before, invariant(&f).await);
    let receipt = results::record(&f.c.env.env.state, &auth, &report)
        .await
        .unwrap();
    assert_eq!(
        receipt.binding, report.binding,
        "the old stopped attempt can still record its exact result"
    );
    assert_eq!(before, invariant(&f).await, "recording is not task release");
}

#[tokio::test]
async fn running_or_missing_stopped_owner_keeps_node_compute_busy() {
    let (f, report) = Box::pin(ready_with_stop(false)).await;
    let before = invariant(&f).await;
    let auth = terminal::auth(&f).await;
    let request = replacement(&f).await;
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT profiled_compute_stopped_valid($1,$2)")
            .bind(report.binding.attempt_id)
            .bind(auth.id())
            .fetch_one(&f.c.env.env.pool)
            .await
            .unwrap()
    );
    assert!(
        registration::register(&f.c.env.env.state, &auth, &request)
            .await
            .is_err()
    );
    assert_eq!(before, invariant(&f).await);
}

/// Whether an open slot poll holds the attempt: a second connection's
/// `FOR UPDATE NOWAIT`, as delivery's attempt fence and worker requests lock it.
async fn poll_holds(f: &terminal::Prepared, attempt: Uuid) -> bool {
    let mut probe = f.c.env.env.pool.begin().await.unwrap();
    for sql in [
        "SELECT 1 FROM execution_attempts WHERE attempt_id=$1 FOR UPDATE NOWAIT",
        "SELECT 1 FROM profiled_authority_hot WHERE attempt_id=$1 FOR UPDATE NOWAIT",
    ] {
        if let Err(e) = sqlx::query(sql).bind(attempt).execute(&mut *probe).await {
            let code = e.as_database_error().and_then(|e| e.code());
            assert_eq!(code.as_deref(), Some("55P03"), "{e}");
            return true;
        }
    }
    false
}

/// The attempts a slot poll pinned as stopped for its slot assignment.
async fn poll_pins(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>) -> Vec<String> {
    let pins: String = sqlx::query_scalar("SELECT current_setting('aq.compute_stopped')")
        .fetch_one(&mut **tx)
        .await
        .unwrap();
    serde_json::from_str::<serde_json::Map<String, Value>>(&pins)
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// The live fault of 2026-11-28: every slot poll locked and re-proved each
/// finished attempt still held for delivery, so delivery's attempt fence on
/// them timed out. A verified stop is now never locked by a poll, and only a
/// poll of its own slot, whose assignment meets it, re-proves and pins it.
#[tokio::test]
async fn a_slot_poll_locks_no_verified_stop_and_pins_only_its_own_slot() {
    let (f, report) = Box::pin(ready()).await;
    let auth = terminal::auth(&f).await;
    let state = &f.c.env.env.state;
    let attempt = report.binding.attempt_id;
    let slot: i32 =
        sqlx::query_scalar("SELECT slot FROM execution_attempt_slots WHERE attempt_id=$1")
            .bind(attempt)
            .fetch_one(&f.c.env.env.pool)
            .await
            .unwrap();
    let slot = slot as u32;
    assert!(!aq_server::test_support::node_busy(state, &auth).await.unwrap());
    let (mut tx, free) = aq_server::test_support::slot_poll_open(state, &auth, slot)
        .await
        .unwrap();
    assert!(free, "a verified stop frees its slot");
    assert!(!poll_holds(&f, attempt).await);
    assert_eq!(poll_pins(&mut tx).await, [attempt.to_string()]);
    tx.rollback().await.unwrap();
    let (mut tx, _) = aq_server::test_support::slot_poll_open(state, &auth, slot + 1)
        .await
        .unwrap();
    assert!(!poll_holds(&f, attempt).await);
    assert!(poll_pins(&mut tx).await.is_empty(), "not re-proved");
    tx.rollback().await.unwrap();
}

/// An attempt without a verified stop may still run: it stays busy, its slot
/// is not free, and a poll still locks it.
#[tokio::test]
async fn a_slot_poll_still_locks_and_counts_an_unverified_stop() {
    let (f, report) = Box::pin(ready_with_stop(false)).await;
    let auth = terminal::auth(&f).await;
    let state = &f.c.env.env.state;
    let attempt = report.binding.attempt_id;
    assert!(aq_server::test_support::node_busy(state, &auth).await.unwrap());
    let (mut tx, free) = aq_server::test_support::slot_poll_open(state, &auth, 1)
        .await
        .unwrap();
    assert!(!free);
    assert!(poll_holds(&f, attempt).await);
    assert!(poll_pins(&mut tx).await.is_empty());
    tx.rollback().await.unwrap();
}
