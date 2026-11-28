//! Typed historical stop proofs gate compute reuse without releasing task ownership.
use crate::{
    error::AppError,
    execution::{
        authority::hot::HotState,
        profiled::{Status, Stop},
    },
};
use aq_admission::LaunchBinding;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

enum Occupancy {
    Busy,
    LostRelease,
    Stopped(serde_json::Value),
}

/// The stopped occupants whose witness the caller's next guarded write needs:
/// every one, or those of one slot (and unslotted ones), which its assignment meets.
pub(crate) enum Pins {
    All,
    Slot(u32),
}

const HELD: &str = "SELECT a.attempt_id,a.kind,l.launch_binding FROM execution_attempts a LEFT JOIN profiled_launches l USING(attempt_id)";
const RELEASED: &str = "EXISTS(SELECT 1 FROM execution_attempt_compute c WHERE c.attempt_id=a.attempt_id AND c.released)";

/// Caller owns the current enrolled-node lock. Bound work and fail closed when
/// history is unavailable; never infer stop from `holds_task` or status alone.
/// Only attempts that may occupy compute are locked. One with a stored release
/// (0185, the SQL stop proof) cannot occupy it again, so it is read unlocked and
/// validated only when pinned: a slot poll must not lock or re-prove every
/// finished attempt that waits for delivery, which fences each of them.
pub(crate) async fn busy_locked(
    tx: &mut Transaction<'_, Postgres>,
    node: Uuid,
    pinned: Pins,
) -> Result<bool, AppError> {
    let stored = aq_machine_control::slots::available(tx)
        .await
        .map_err(crate::machine_api::mapped)?;
    let occupants = if stored {
        format!(
            "{HELD} WHERE a.node_id=$1 AND a.holds_task AND NOT {RELEASED} ORDER BY a.attempt_id LIMIT 1025 FOR UPDATE OF a"
        )
    } else {
        format!(
            "{HELD} WHERE a.node_id=$1 AND a.holds_task ORDER BY a.attempt_id LIMIT 1025 FOR UPDATE OF a"
        )
    };
    let rows = sqlx::query(&occupants)
        .bind(node)
        .fetch_all(&mut **tx)
        .await
        .map_err(|_| AppError::Internal)?;
    let slot = match pinned {
        Pins::All => None,
        Pins::Slot(slot) => Some(slot as i32),
    };
    let released = if stored {
        sqlx::query(&format!("{HELD} LEFT JOIN execution_attempt_slots s USING(attempt_id) WHERE a.node_id=$1 AND a.holds_task AND {RELEASED} AND ($2::INTEGER IS NULL OR s.slot IS NULL OR s.slot=$2) ORDER BY a.attempt_id LIMIT 1025"))
            .bind(node)
            .bind(slot)
            .fetch_all(&mut **tx)
            .await
            .map_err(|_| AppError::Internal)?
    } else {
        Vec::new()
    };
    if rows.len() > 1024 || released.len() > 1024 {
        return Ok(true);
    }
    let mut pins = serde_json::Map::new();
    let mut busy = false;
    for (row, lock) in rows
        .iter()
        .map(|row| (row, true))
        .chain(released.iter().map(|row| (row, false)))
    {
        match occupancy_locked(tx, node, row, lock).await? {
            Occupancy::Busy => busy = true,
            Occupancy::LostRelease => {}
            Occupancy::Stopped(pin) => {
                pins.insert(row.get::<Uuid, _>("attempt_id").to_string(), pin);
            }
        }
    }
    // Transaction-local witness for the incarnation SQL guard, never persisted
    // authority. SQL independently joins every exact immutable stopped owner.
    sqlx::query("SELECT set_config('aq.compute_stopped',$1,true)")
        .bind(serde_json::Value::Object(pins).to_string())
        .execute(&mut **tx)
        .await
        .map_err(|_| AppError::Internal)?;
    Ok(busy)
}

async fn occupancy_locked(
    tx: &mut Transaction<'_, Postgres>,
    node: Uuid,
    row: &sqlx::postgres::PgRow,
    lock: bool,
) -> Result<Occupancy, AppError> {
    if row.get::<String, _>("kind") != "profiled_v1" {
        return Ok(Occupancy::Busy);
    }
    let id: Uuid = row.get("attempt_id");
    let Some(binding) = row.get::<Option<serde_json::Value>, _>("launch_binding") else {
        return Ok(Occupancy::Busy);
    };
    let binding: LaunchBinding = serde_json::from_value(binding)
        .map_err(|_| AppError::Conflict("compute history binding unavailable".into()))?;
    let hot = if lock {
        "SELECT state FROM profiled_authority_hot WHERE attempt_id=$1 FOR UPDATE"
    } else {
        "SELECT state FROM profiled_authority_hot WHERE attempt_id=$1"
    };
    let hot: Option<serde_json::Value> = sqlx::query_scalar(hot)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| AppError::Internal)?;
    let Some(hot) = hot else {
        return Ok(Occupancy::Busy);
    };
    let hot: HotState = serde_json::from_value(hot)
        .map_err(|_| AppError::Conflict("compute history unavailable".into()))?;
    hot.validate_against(&binding)
        .map_err(|_| AppError::Conflict("compute history invalid".into()))?;
    let (Status::Finished, Stop::Stopped { receipt }, Some(grant)) =
        (hot.status, &hot.stop, &hot.grant)
    else {
        return Ok(Occupancy::Busy);
    };
    // A lost release frees compute while B4 can retain the task.
    if receipt.id.as_str().starts_with("aq.lost-attempt-release/") {
        return Ok(Occupancy::LostRelease);
    }
    let pin =
        super::results::compute_stop_locked(tx, node, &binding, grant.grant_id, receipt).await?;
    let valid: bool = sqlx::query_scalar("SELECT profiled_compute_stopped_valid($1,$2)")
        .bind(id)
        .bind(node)
        .fetch_one(&mut **tx)
        .await
        .map_err(|_| AppError::Internal)?;
    if !valid {
        return Ok(Occupancy::Busy);
    }
    Ok(Occupancy::Stopped(pin))
}

/// Validate every occupant and the stopped ones of `slot`, whose witnesses its
/// assignment needs, retaining them even when a sibling is unknown.
pub(crate) async fn free_locked(
    tx: &mut Transaction<'_, Postgres>,
    node: Uuid,
    slot: u32,
) -> Result<bool, AppError> {
    let busy = busy_locked(tx, node, Pins::Slot(slot)).await?;
    if !aq_machine_control::slots::available(tx)
        .await
        .map_err(crate::machine_api::mapped)?
    {
        return Ok(slot == 1 && !busy);
    }
    aq_machine_control::slots::poll_free(tx, node, slot)
        .await
        .map_err(crate::machine_api::mapped)
}

pub(crate) fn manual_witness<'a, 'c>(
    tx: &'a mut Transaction<'c, Postgres>,
    node: Uuid,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = aq_manual_sessions::Result<()>> + Send + 'a>>
{
    Box::pin(async move {
        busy_locked(tx, node, Pins::All)
            .await
            .map(|_| ())
            .map_err(|_| {
                aq_manual_sessions::Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "Compute history is unavailable",
                )
            })
    })
}
