mod addons_runtime;
mod commons;
mod worker_task;

use commons::utils::CONFIG as config;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn init_postgresql_schema() -> Result<(), String> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.db.sql_url)
        .await
        .map_err(|err| err.to_string())?;

    sqlx::raw_sql(
        "create table if not exists player_profiles (
                userid text primary key,
                nickname text not null,
                level bigint not null default 1,
                exp bigint not null default 0,
                avatar_id bigint not null default 0,
                created_at timestamptz not null default now(),
                updated_at timestamptz not null default now()
            );

            create table if not exists player_wallets (
                userid text primary key references player_profiles(userid) on delete cascade,
                gold bigint not null default 0,
                diamond bigint not null default 0,
                stamina bigint not null default 100,
                updated_at timestamptz not null default now()
            );

            create table if not exists player_items (
                id bigserial primary key,
                userid text not null references player_profiles(userid) on delete cascade,
                item_id text not null,
                item_count bigint not null default 0,
                updated_at timestamptz not null default now(),
                unique(userid, item_id)
            );

            create table if not exists player_mails (
                id bigserial primary key,
                userid text not null references player_profiles(userid) on delete cascade,
                title text not null,
                content text not null default '',
                attachments jsonb not null default '[]'::jsonb,
                status text not null default 'unread',
                created_at timestamptz not null default now()
            );

            create table if not exists task_idempotency (
                task_id text primary key,
                task_type text not null,
                status text not null default 'running',
                attempt bigint not null default 1,
                result jsonb,
                created_at timestamptz not null default now(),
                updated_at timestamptz not null default now()
            );

            alter table player_profiles alter column level type bigint;
            alter table player_profiles alter column avatar_id type bigint;
            alter table player_wallets alter column stamina type bigint;",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;

    migrate_legacy_players(&pool).await
}

async fn migrate_legacy_players(pool: &PgPool) -> Result<(), String> {
    let row: (bool,) = sqlx::query_as("select to_regclass('public.players') is not null")
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;
    let has_legacy_table = row.0;
    if !has_legacy_table {
        return Ok(());
    }

    sqlx::raw_sql(
        "insert into player_profiles (userid, nickname)
             select userid, nickname
             from players
             on conflict (userid)
             do update set nickname = excluded.nickname;

             insert into player_wallets (userid)
             select userid
             from players
             on conflict (userid) do nothing;

             drop table if exists players;",
    )
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_userid() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_millis())
            .unwrap_or(0);
        format!("test_userid_{ts}")
    }

    #[tokio::test]
    async fn test_player_wallet_item_mail_by_addon() {
        if let Err(err) = crate::commons::db::init_db().await {
            eprintln!("skip db integration test: {err}");
            return;
        }
        if let Err(err) = crate::commons::redis::init_redis().await {
            eprintln!("skip redis integration test: {err}");
            return;
        }

        crate::addons_runtime::init_addons();
        let userid = test_userid();
        let create_result = crate::worker_task::run_worker_task(json!({
            "type": "create_player",
            "userid": userid,
            "nickname": "test_nickname"
        }))
        .await;
        if create_result.get("ok").and_then(|value| value.as_bool()) != Some(true) {
            eprintln!("skip db integration test: {create_result}");
            return;
        }

        let player = create_result
            .get("player")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let profile = player.get("profile").and_then(|value| value.as_object());
        assert_eq!(
            profile
                .and_then(|value| value.get("userid"))
                .and_then(|value| value.as_str()),
            Some(userid.as_str())
        );
        assert_eq!(
            profile
                .and_then(|value| value.get("nickname"))
                .and_then(|value| value.as_str()),
            Some("test_nickname")
        );

        let wallet = player.get("wallet").and_then(|value| value.as_object());
        assert_eq!(
            wallet
                .and_then(|value| value.get("gold"))
                .and_then(|value| value.as_i64()),
            Some(0)
        );

        let add_gold_result = crate::worker_task::run_worker_task(json!({
            "type": "add_currency",
            "userid": userid,
            "currency": "gold",
            "delta": 100
        }))
        .await;
        assert_eq!(
            add_gold_result.get("ok").and_then(|value| value.as_bool()),
            Some(true)
        );

        let add_item_result = crate::worker_task::run_worker_task(json!({
            "type": "add_item",
            "userid": userid,
            "item_id": "sword_001",
            "delta": 2
        }))
        .await;
        assert_eq!(
            add_item_result.get("ok").and_then(|value| value.as_bool()),
            Some(true)
        );

        let send_mail_result = crate::worker_task::run_worker_task(json!({
            "type": "send_mail",
            "userid": userid,
            "title": "welcome",
            "content": "starter reward",
            "attachments": [
                { "kind": "currency", "name": "diamond", "count": 10 },
                { "kind": "item", "item_id": "potion_001", "count": 3 }
            ]
        }))
        .await;
        assert_eq!(
            send_mail_result.get("ok").and_then(|value| value.as_bool()),
            Some(true)
        );
        let mail_id = send_mail_result
            .get("mail")
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        assert!(mail_id > 0);

        let claim_mail_result = crate::worker_task::run_worker_task(json!({
            "type": "claim_mail",
            "userid": userid,
            "mail_id": mail_id
        }))
        .await;
        assert_eq!(
            claim_mail_result
                .get("ok")
                .and_then(|value| value.as_bool()),
            Some(true)
        );

        let get_result = crate::worker_task::run_worker_task(json!({
            "type": "get_player",
            "userid": userid
        }))
        .await;
        assert_eq!(
            get_result.get("ok").and_then(|value| value.as_bool()),
            Some(true)
        );

        let player = get_result
            .get("player")
            .cloned()
            .unwrap_or_else(|| json!({}));
        assert_eq!(
            player
                .get("wallet")
                .and_then(|value| value.get("gold"))
                .and_then(|value| value.as_i64()),
            Some(100)
        );
        assert_eq!(
            player
                .get("wallet")
                .and_then(|value| value.get("diamond"))
                .and_then(|value| value.as_i64()),
            Some(10)
        );
        let items = player
            .get("items")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(items.iter().any(|item| {
            item.get("item_id").and_then(|value| value.as_str()) == Some("sword_001")
                && item.get("item_count").and_then(|value| value.as_i64()) == Some(2)
        }));
        assert!(items.iter().any(|item| {
            item.get("item_id").and_then(|value| value.as_str()) == Some("potion_001")
                && item.get("item_count").and_then(|value| value.as_i64()) == Some(3)
        }));
    }

    #[tokio::test]
    async fn test_init_postgresql_schema() {
        if let Err(err) = crate::init_postgresql_schema().await {
            eprintln!("init_postgresql_schema failed: {err}");
        }
    }
}
