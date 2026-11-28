use super::*;
use aq_delivery::commands::{GitFailure, GitPort, RefDeletion, RefPublication, RetainedRef};
use aq_delivery::{
    commands::{Delivery, DeliveryTarget, FrozenCommand, GitTarget},
    retained_publication as publication,
};
use publication::Identity;

struct Journal {
    head: Mutex<Option<String>>,
    reads: AtomicUsize,
    refused: AtomicBool,
    unavailable: AtomicBool,
}
#[async_trait::async_trait]
impl GitPort for Journal {
    async fn inspect_publication(
        &self,
        original: Uuid,
        _: &GitTarget,
        _: &str,
        _: Option<&str>,
        _: &str,
    ) -> std::result::Result<Value, GitFailure> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GitFailure {
                code: "transport".into(),
                message: "aq-git did not answer".into(),
                transient: true,
                details: Value::Null,
            });
        }
        if self.refused.load(Ordering::SeqCst) {
            return Ok(
                json!({"operation_id":original,"generation":7,"status":"complete","phase":"complete","outcome":"rejected","reason":"remote_rejected","rejection_class":"hook_declined","published":false}),
            );
        }
        Ok(
            json!({"operation_id":original,"generation":7,"status":"pending","phase":"indeterminate","published":false}),
        )
    }
    async fn reconcile_publication(
        &self,
        _: Uuid,
        _: &GitTarget,
        _: Uuid,
        generation: u64,
        source: &str,
    ) -> std::result::Result<Value, GitFailure> {
        assert_eq!(generation, 7, "generation is frozen from original journal");
        self.reads.fetch_add(1, Ordering::SeqCst);
        let head = self.head.lock().unwrap();
        Ok(if head.as_deref() == Some(source) {
            json!({"state":"published","head":source,"caused_by_this_operation":false})
        } else {
            json!({"state":"held","reason":"operation_indeterminate"})
        })
    }
    async fn retain_ref(
        &self,
        _: Uuid,
        _: &GitTarget,
        _: &str,
    ) -> std::result::Result<RetainedRef, GitFailure> {
        panic!("recovery must not retain a substitute")
    }
    async fn publish_ref(
        &self,
        _: Uuid,
        _: &GitTarget,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
    ) -> std::result::Result<RefPublication, GitFailure> {
        panic!("recovery must never publish or force")
    }
    async fn delete_ref(
        &self,
        _: Uuid,
        _: &GitTarget,
        _: &str,
        _: Option<&str>,
    ) -> std::result::Result<RefDeletion, GitFailure> {
        panic!("recovery must not delete")
    }
    async fn delete_central_ref(
        &self,
        _: Uuid,
        _: &GitTarget,
        _: &str,
        _: Option<&str>,
    ) -> std::result::Result<RefDeletion, GitFailure> {
        panic!("recovery must not release source")
    }
}
/// The host retains only a completion its catalog contract accepts; any other
/// stays running forever.
fn retainable(step: &aq_delivery::commands::Completion) {
    let registry = aq_server::playbooks::review_domain::CatalogProfile::Host10.registry();
    let contract = registry.commands["branch.publish"]
        .asynchronous
        .clone()
        .unwrap();
    contract
        .completion
        .validate(&json!({"outcome": step.outcome, "output": step.output}))
        .unwrap();
}
struct Case {
    env: AutoEnv,
    pool: sqlx::PgPool,
    id: Identity,
    target: DeliveryTarget,
    delivery: Delivery,
    journal: Arc<Journal>,
    ns: Uuid,
}
impl Case {
    async fn new() -> Self {
        let env = Box::pin(auto_env("not_required", axum::http::StatusCode::OK)).await;
        let pool = env.f.db.pool().await;
        let report = env.report.report_id;
        let project = env.f.selection.lineage.project_id;
        let revision: String = sqlx::query_scalar(
            "SELECT report_digest FROM profiled_result_reports WHERE report_id=$1",
        )
        .bind(report)
        .fetch_one(&pool)
        .await
        .unwrap();
        let repo: Uuid =
            sqlx::query_scalar("SELECT repository_id FROM delivery_git_grants WHERE attempt_id=$1")
                .bind(env.report.binding.attempt_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let id = publication::identity(&pool, project, report, &revision, repo)
            .await
            .unwrap();
        let target = DeliveryTarget {
            git: GitTarget {
                repository_id: repo,
                binding_generation: id.binding_generation,
            },
            github: aq_delivery::GitHubRepository {
                owner: "example".into(),
                name: "retained".into(),
            },
            default_branch: "main".into(),
        };
        let journal = Arc::new(Journal {
            head: Mutex::new(Some(id.head.clone())),
            reads: AtomicUsize::new(0),
            refused: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
        });
        let delivery = Delivery::new(journal.clone(), Arc::new(aq_delivery::UnconfiguredGitHub));
        let ns = Uuid::new_v4();
        sqlx::query("INSERT INTO playbook_namespaces(namespace_id,generation,name,workspace_id,project_id,owner_user_id,catalog_digest,source_digest) VALUES($1,1,'recovery',$2,$3,$4,repeat('a',64),repeat('b',64))")
            .bind(ns).bind(env.f.selection.lineage.workspace_id).bind(project).bind(env.f.actor).execute(&pool).await.unwrap();
        Self {
            env,
            pool,
            id,
            target,
            delivery,
            journal,
            ns,
        }
    }
    fn request(&self) -> Value {
        let id = &self.id;
        json!({"project":id.project_id,"task":id.task_id,"result_head":id.head,"publication_target":self.target,
            "expected_old":null,"answer_note":"remote_rejected",
            "result_ready":{"project_id":id.project_id,"task_id":id.task_id,"attempt_id":id.attempt_id,"result_id":id.report_id}})
    }
    async fn original(&self) -> Uuid {
        self.original_with(self.request(), json!({"reason":"unreachable"}))
            .await
    }
    async fn original_with(&self, request: Value, output: Value) -> Uuid {
        let op = Uuid::new_v4();
        sqlx::query("INSERT INTO playbook_host_operations(operation_id,namespace_id,generation,intent_id,command_name,subject_id,subject_revision,request,status,output,created_at_ms,completed_at_ms) VALUES($1,$2,1,$3,'branch.publish',$4,0,$5,'refused',$6,$7,$7)")
            .bind(op).bind(self.ns).bind(op.to_string()).bind(self.id.task_id.to_string()).bind(request).bind(output).bind(NOW as i64).execute(&self.pool).await.unwrap();
        op
    }
    async fn recover(
        &self,
        op: Uuid,
        original: Uuid,
    ) -> std::result::Result<Value, aq_delivery::DeliveryError> {
        publication::reconcile(
            &self.pool,
            &self.delivery,
            &self.target,
            &self.id,
            op,
            original,
            self.env.f.actor,
        )
        .await
    }
    async fn recover_step(&self, frozen: &Value) -> aq_delivery::commands::Completion {
        let op = Uuid::new_v4();
        publication::recover_step(
            &self.pool,
            &mut self.pool.acquire().await.unwrap(),
            &self.delivery,
            &self.target,
            FrozenCommand::new(op, "branch.publish", frozen),
        )
        .await
        .unwrap()
    }
    /// A later delivery run's `branch.publish` for the same result, stepped in a
    /// transaction that holds the attempt lock as the host's step does.
    async fn duplicate(&self) -> aq_delivery::commands::Completion {
        let request = self.request();
        let duplicate = self.original().await;
        let mut tx = self.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL lock_timeout='10s'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT 1 FROM execution_attempts WHERE attempt_id=$1 FOR UPDATE")
            .bind(self.id.attempt_id)
            .execute(&mut *tx)
            .await
            .unwrap();
        let step = aq_delivery::commands::run_step(
            &self.pool,
            &mut tx,
            aq_delivery::commands::DeliveryRuntime {
                service: Some(&self.delivery),
                target: Some(&self.target),
            },
            FrozenCommand::new(duplicate, "branch.publish", &request),
            NOW as i64,
        )
        .await
        .unwrap()
        .unwrap();
        tx.commit().await.unwrap();
        step
    }
    /// The first run's dispatch, as `reserve` commits it before calling Git.
    async fn dispatch(&self) -> Uuid {
        let original = self.original().await;
        let request = self.request();
        assert!(
            publication::reserve(
                &self.pool,
                FrozenCommand::new(original, "branch.publish", &request),
                &self.target
            )
            .await
            .unwrap()
        );
        original
    }
    async fn recoveries(&self) -> Vec<Value> {
        sqlx::query_scalar("SELECT r.evidence || jsonb_build_object('actor',r.request->'actor_id','journal',q.publisher) FROM retained_publication_recoveries r JOIN retained_publication_recovery_requests q USING(operation_id) WHERE r.report_id=$1 ORDER BY r.recorded_at")
            .bind(self.id.report_id)
            .fetch_all(&self.pool)
            .await
            .unwrap()
    }
}
#[tokio::test]
async fn exact_result_refusal_and_uncertain_success_recover_an_idempotent_receipt_without_republication()
 {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    let later = c.original().await;
    let operation = Uuid::new_v4();
    let receipt = c.recover(operation, original).await.unwrap();
    assert_eq!(receipt["observation"]["state"], "published");
    assert_eq!(receipt["observation"]["caused_by_this_operation"], false);
    assert_eq!(receipt["publication"]["diagnostic"], "remote_rejected");
    assert_eq!(
        receipt["publisher"]["status"], "pending",
        "original journal is bound"
    );
    assert_eq!(c.recover(operation, original).await.unwrap(), receipt);
    assert_eq!(c.journal.reads.load(Ordering::SeqCst), 1);
    assert!(matches!(
        c.recover(operation, later).await,
        Err(aq_delivery::DeliveryError::IdempotencyConflict)
    ));
    assert!(c.recover(original, original).await.is_err());
    // Every original and subsequent operation remains available and refused.
    let page = publication::inspect(&c.pool, &c.id, None).await.unwrap();
    assert_eq!(page["operations"].as_array().unwrap().len(), 2);
    assert!(
        page["operations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["status"] == "refused")
    );
    let new = c.original().await;
    let frozen = c.request();
    assert!(
        !publication::reserve(
            &c.pool,
            FrozenCommand::new(new, "branch.publish", &frozen),
            &c.target
        )
        .await
        .unwrap()
    );
    let completion = c.recover_step(&frozen).await;
    assert_eq!(completion.outcome, "published");
    retainable(&completion);
    assert!(
        c.env.holds_task().await,
        "recovery does not release or accept"
    );
    assert_eq!(
        aq_delivery::commands::published_head(&c.pool, c.id.task_id)
            .await
            .unwrap(),
        Some(c.id.head.clone())
    );
    assert!(
        sqlx::query("DELETE FROM retained_publication_recoveries WHERE operation_id=$1")
            .bind(operation)
            .execute(&c.pool)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn absent_or_moved_remote_retains_rejection_and_requires_a_new_observation_after_correction()
{
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    for head in [None, Some("b".repeat(40))] {
        *c.journal.head.lock().unwrap() = head.clone();
        let op = Uuid::new_v4();
        let refused = c.recover(op, original).await.unwrap();
        assert_eq!(refused["observation"]["state"], "held");
        assert_eq!(refused["observation"]["reason"], "operation_indeterminate");
        assert!(refused["publication"]["expected_old"].is_null());
        *c.journal.head.lock().unwrap() = Some(c.id.head.clone());
        assert_eq!(
            c.recover(op, original).await.unwrap(),
            refused,
            "same ID freezes the original observation"
        );
    }
    assert_eq!(
        c.recover(Uuid::new_v4(), original).await.unwrap()["observation"]["state"],
        "published"
    );
    assert_eq!(c.journal.reads.load(Ordering::SeqCst), 3);
    assert!(c.env.holds_task().await);
}
#[tokio::test]
async fn wrong_project_repository_result_revision_binding_and_cancelled_claim_are_refused() {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    for (project, report, revision, repo) in [
        (
            Uuid::new_v4(),
            c.id.report_id,
            c.id.revision.clone(),
            c.id.repository_id,
        ),
        (
            c.id.project_id,
            Uuid::new_v4(),
            c.id.revision.clone(),
            c.id.repository_id,
        ),
        (
            c.id.project_id,
            c.id.report_id,
            "f".repeat(64),
            c.id.repository_id,
        ),
        (
            c.id.project_id,
            c.id.report_id,
            c.id.revision.clone(),
            Uuid::new_v4(),
        ),
    ] {
        assert!(
            publication::identity(&c.pool, project, report, &revision, repo)
                .await
                .is_err()
        );
    }
    let mut wrong = c.target.clone();
    wrong.git.binding_generation += 1;
    assert!(
        publication::reconcile(
            &c.pool,
            &c.delivery,
            &wrong,
            &c.id,
            Uuid::new_v4(),
            original,
            c.env.f.actor
        )
        .await
        .is_err()
    );
    let mut tx = c.pool.begin().await.unwrap();
    sqlx::query("SET LOCAL session_replication_role=replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_attempts SET holds_task=false WHERE attempt_id=$1")
        .bind(c.id.attempt_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(c.recover(Uuid::new_v4(), original).await.is_err());
    assert_eq!(c.journal.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn definitive_rejection_is_evidence_not_permission_to_retry_or_release() {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    c.journal.refused.store(true, Ordering::SeqCst);
    let receipt = c.recover(Uuid::new_v4(), original).await.unwrap();
    assert_eq!(receipt["publisher"]["reason"], "remote_rejected");
    assert_eq!(receipt["publisher"]["rejection_class"], "hook_declined");
    assert_eq!(receipt["observation"]["state"], "held");
    assert_eq!(
        c.journal.reads.load(Ordering::SeqCst),
        0,
        "definitive rejection cannot call operation.reconcile"
    );
    let request = c.request();
    let replay = c.recover_step(&request).await;
    assert_eq!(replay.outcome, "refused");
    assert!(c.env.holds_task().await);
}
#[tokio::test]
async fn original_operation_history_is_bounded_and_can_be_read_without_dropping_identities() {
    let c = Box::pin(Case::new()).await;
    let mut ids = Vec::new();
    for _ in 0..27 {
        ids.push(c.original().await);
    }
    ids.sort();
    let first = publication::inspect(&c.pool, &c.id, None).await.unwrap();
    assert_eq!(first["operations"].as_array().unwrap().len(), 25);
    let cursor = first["next_after"].as_str().unwrap().parse().unwrap();
    let last = publication::inspect(&c.pool, &c.id, Some(cursor))
        .await
        .unwrap();
    assert_eq!(last["operations"].as_array().unwrap().len(), 2);
    assert!(last["next_after"].is_null());
    let read: Vec<Uuid> = first["operations"]
        .as_array()
        .unwrap()
        .iter()
        .chain(last["operations"].as_array().unwrap())
        .map(|o| o["operation_id"].as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(read, ids);
}
#[tokio::test]
async fn a_lost_receipt_reuses_the_durably_frozen_publisher_generation() {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    let operation = Uuid::new_v4();
    let request = json!({"identity":c.id,"target":c.target,"publication_operation_id":original,"actor_id":c.env.f.actor});
    let publisher = json!({"operation_id":original,"generation":7,"status":"pending","phase":"indeterminate","published":false});
    sqlx::query("INSERT INTO retained_publication_recovery_requests(operation_id,publication_operation_id,report_id,request,publisher) VALUES($1,$2,$3,$4,$5)")
        .bind(operation).bind(original).bind(c.id.report_id).bind(&request).bind(publisher).execute(&c.pool).await.unwrap();
    // Even if a later journal read would be a definitive rejection, the already
    // frozen request replays its original observation, under generation seven.
    c.journal.refused.store(true, Ordering::SeqCst);
    let receipt = c.recover(operation, original).await.unwrap();
    assert_eq!(receipt["publisher"]["generation"], 7);
    assert_eq!(receipt["observation"]["state"], "published");
    assert_eq!(c.recover(operation, original).await.unwrap(), receipt);
    assert_eq!(c.journal.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn committed_dispatch_marker_prevents_repeated_or_substitute_push_after_a_crash() {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    let request = c.request();
    let frozen = FrozenCommand::new(original, "branch.publish", &request);
    assert!(
        publication::reserve(&c.pool, frozen, &c.target)
            .await
            .unwrap()
    );
    assert!(
        !publication::reserve(&c.pool, frozen, &c.target)
            .await
            .unwrap()
    );
    // Simulate a driver restart with a committed dispatch but no completion,
    // and a remote that does not hold the head. Any publish would panic.
    *c.journal.head.lock().unwrap() = None;
    let mut db = c.pool.acquire().await.unwrap();
    let step = aq_delivery::commands::run_step(
        &c.pool,
        &mut db,
        aq_delivery::commands::DeliveryRuntime {
            service: Some(&c.delivery),
            target: Some(&c.target),
        },
        frozen,
        NOW as i64,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(step.outcome, "refused");
    let substitute = c.original().await;
    assert!(
        !publication::reserve(
            &c.pool,
            FrozenCommand::new(substitute, "branch.publish", &request),
            &c.target
        )
        .await
        .unwrap()
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM retained_publication_dispatches WHERE report_id=$1",
    )
    .bind(c.id.report_id)
    .fetch_one(&c.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        c.journal.reads.load(Ordering::SeqCst),
        1,
        "only an observation"
    );
    assert!(c.env.holds_task().await);
}

#[tokio::test]
async fn a_read_only_preflight_failure_does_not_consume_the_publication_dispatch() {
    let c = Box::pin(Case::new()).await;
    let original = c.original().await;
    let mut request = c.request();
    request["pull_request"] = json!(7);
    let frozen = FrozenCommand::new(original, "branch.publish", &request);
    for _ in 0..2 {
        let mut db = c.pool.acquire().await.unwrap();
        let result = aq_delivery::commands::run_step(
            &c.pool,
            &mut db,
            aq_delivery::commands::DeliveryRuntime {
                service: Some(&c.delivery),
                target: Some(&c.target),
            },
            frozen,
            NOW as i64,
        )
        .await
        .unwrap();
        assert!(
            result.is_none(),
            "unavailable PR observation remains retryable before dispatch"
        );
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM retained_publication_dispatches WHERE report_id=$1",
    )
    .bind(c.id.report_id)
    .fetch_one(&c.pool)
    .await
    .unwrap();
    assert_eq!(
        count, 0,
        "only the actual Git call consumes the dispatch marker"
    );
    assert!(
        publication::reserve(&c.pool, frozen, &c.target)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_proven_preflight_refusal_does_not_fence_a_fresh_request_but_a_dispatch_still_does() {
    let c = Box::pin(Case::new()).await;
    let mut preflight = c.request();
    preflight["answer_note"] = json!("publication_not_dispatched");
    c.original_with(preflight.clone(), json!({"reason":"no_branch"}))
        .await;
    let next = c.original().await;
    assert!(
        publication::reserve(
            &c.pool,
            FrozenCommand::new(next, "branch.publish", &c.request()),
            &c.target,
        )
        .await
        .unwrap()
    );
    // Even a proven non-dispatch cannot erase a committed dispatch fence.
    let substitute = c
        .original_with(preflight.clone(), json!({"reason":"no_branch"}))
        .await;
    assert!(
        !publication::reserve(
            &c.pool,
            FrozenCommand::new(substitute, "branch.publish", &preflight),
            &c.target,
        )
        .await
        .unwrap()
    );
    assert!(c.env.holds_task().await);
}

#[tokio::test]
async fn a_duplicate_publication_recovers_the_proven_original_head_without_another_push() {
    let c = Box::pin(Case::new()).await;
    let original = c.dispatch().await;
    // Journal's publish_ref panics: any second push fails this test.
    let step = c.duplicate().await;
    assert_eq!(step.outcome, "published");
    retainable(&step);
    assert_eq!(step.output, json!({"head": c.id.head}));
    let recoveries = c.recoveries().await;
    assert_eq!(recoveries.len(), 1);
    let receipt = &recoveries[0];
    assert_eq!(receipt["publication"]["operation_id"], json!(original));
    assert_eq!(receipt["observation"]["state"], "published");
    assert_eq!(receipt["identity"]["head"], json!(c.id.head));
    assert_eq!(receipt["actor"], json!(Uuid::nil()), "the host observed it");
    assert_eq!(receipt["journal"]["generation"], 7);
    // A later run replays the recorded receipt without reading Git again.
    assert_eq!(c.duplicate().await.outcome, "published");
    assert_eq!(c.journal.reads.load(Ordering::SeqCst), 1);
    assert_eq!(c.recoveries().await.len(), 1);
    // The owner endpoint still observes under its own ID after the host did.
    let owner = c.recover(Uuid::new_v4(), original).await.unwrap();
    assert_eq!(owner["observation"]["state"], "published");
    assert!(
        c.env.holds_task().await,
        "recovery does not release or accept"
    );
    assert_eq!(
        aq_delivery::commands::published_head(&c.pool, c.id.task_id)
            .await
            .unwrap(),
        Some(c.id.head.clone())
    );
}

#[tokio::test]
async fn a_duplicate_without_journal_proof_still_requires_reconciliation_until_proven() {
    let c = Box::pin(Case::new()).await;
    c.dispatch().await;
    c.journal.unavailable.store(true, Ordering::SeqCst);
    let unavailable = c.duplicate().await;
    *c.journal.head.lock().unwrap() = Some("b".repeat(40));
    c.journal.unavailable.store(false, Ordering::SeqCst);
    let moved = c.duplicate().await;
    for step in [&unavailable, &moved] {
        retainable(step);
        assert_eq!(step.outcome, "refused");
        assert_eq!(step.output["reason"], "unreachable");
        assert_eq!(step.diagnostic, Some("reconciliation_required"));
    }
    let held = c.recoveries().await;
    assert_eq!(held.len(), 2, "each observation is recorded");
    assert_eq!(held[0]["journal"]["state"], "unavailable");
    assert!(held.iter().all(|r| r["observation"]["state"] == "held"));
    assert!(c.env.holds_task().await);
    // A later announcement observes again and heals once the head is proven.
    *c.journal.head.lock().unwrap() = Some(c.id.head.clone());
    assert_eq!(c.duplicate().await.outcome, "published");
}
