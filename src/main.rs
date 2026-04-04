use salvo::{prelude::*, serve_static::static_embed};
mod addons_runtime;
mod commons;
mod distributed;
mod router;
mod worker_task;
use commons::db::init_db;
use commons::utils::CONFIG as config;

#[derive(rust_embed::RustEmbed)]
#[folder = "src/web"]
struct Assets;

#[tokio::main]
async fn main() {
    if let Err(err) = init_db().await {
        panic!("init db failed: {err}");
    }
    if let Err(err) = commons::redis::init_redis().await {
        panic!("init redis failed: {err}");
    }
    addons_runtime::init_addons();
    distributed::start_worker_client();

    let router = Router::new()
        .push(router::config_router())
        .push(
            Router::with_path("/web/{**path}").get(static_embed::<Assets>().fallback("index.html")),
        );
    let host = config.host.clone();
    dbg!(&config);
    println!("http://{}", host);

    let service = Service::new(router).catcher(router::config_catcher());
    let acceptor = TcpListener::new(host).bind().await;
    Server::new(acceptor).serve(service).await;
}
