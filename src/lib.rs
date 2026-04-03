mod commons;

mod tests{
    #[tokio::test]
    async fn test_create_player_info(){
        let _ = crate::commons::db::init_db().await;
        let result = crate::commons::db::create_player_info("test_userid", "test_nickname").await;
        assert!(result.is_ok());
        let player_info = result.unwrap();
        assert_eq!(player_info.get("userid").and_then(|v| v.as_str()), Some("test_userid"));
        assert_eq!(player_info.get("nickname").and_then(|v| v.as_str()), Some("test_nickname"));
    }
}