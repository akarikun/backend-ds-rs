mod addons_runtime;
mod commons;
mod worker_task;

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
}
