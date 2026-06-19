use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use scraper::{ElementRef, Html as ScrapHtml, Selector};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Clone)]
struct CrawlConfig {
    opensearch_url: String,
    username: String,
    password: String,
    user_agent: String,
    max_depth: usize,
    re_crawl_days: i64, // Po koľkých dňoch sa má stránka znova indexovať
}

#[derive(Serialize, Clone)]
struct CrawlStats {
    pages_crawled: usize,
    pages_queued: usize,
    errors_count: usize,
    current_depth: usize,
}

struct AppState {
    config: CrawlConfig,
    stats: CrawlStats,
    queue: VecDeque<(String, usize)>,
    visited: HashMap<String, DateTime<Utc>>, // HashMap ukladá URL a presný čas návštevy
    logs: Vec<String>,
    is_running: bool,
}

type SharedState = Arc<RwLock<AppState>>;

#[derive(Deserialize)]
struct UrlImportRequest {
    urls: Vec<String>,
}

#[tokio::main]
async fn main() {
    // Inicializácia zdieľaného stavu aplikácie
    let default_state = Arc::new(RwLock::new(AppState {
        config: CrawlConfig {
            opensearch_url: "https://localhost:9200".to_string(),
            username: "admin".to_string(),
            password: "QmaySearchEngine2026!".to_string(),
            user_agent: "QmayBot/4.0 (Enterprise)".to_string(),
            max_depth: 3,
            re_crawl_days: 7, // Stránky sa re-indexujú po 7 dňoch
        },
        stats: CrawlStats {
            pages_crawled: 0,
            pages_queued: 0,
            errors_count: 0,
            current_depth: 0,
        },
        queue: VecDeque::new(),
        visited: HashMap::new(),
        logs: vec!["[SYSTEM] Engine pripravený na produkciu (Anti-Dupe & TTL aktivované).".to_string()],
        is_running: false,
    }));

    // Vysoko-výkonný HTTP klient s Keep-Alive
    let http_client = Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(10))
        .pool_max_idle_per_host(10)
        .build()
        .unwrap();

    let crawler_state = default_state.clone();
    let crawler_client = http_client.clone();

    // Hlavná asynchrónna slučka crawlera (Background Worker)
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            let mut is_running = false;
            {
                if let Ok(state) = crawler_state.try_read() {
                    is_running = state.is_running;
                }
            }

            if is_running {
                let mut target_url = None;
                let mut depth = 0;
                let mut config = None;

                if let Ok(mut state) = crawler_state.try_write() {
                    if let Some((url, d)) = state.queue.pop_front() {
                        target_url = Some(url);
                        depth = d;
                        config = Some(state.config.clone());
                        state.stats.current_depth = depth;
                        state.stats.pages_queued = state.queue.len();
                    } else {
                        state.is_running = false;
                        state.logs.push("[SYSTEM] Fronta bola kompletne spracovaná.".to_string());
                    }
                }

                if let (Some(url), Some(cfg)) = (target_url, config) {
                    if depth > cfg.max_depth {
                        continue;
                    }

                    match crawl_and_index(&crawler_client, &url, depth, &cfg, crawler_state.clone()).await {
                        Ok(_) => {
                            if let Ok(mut state) = crawler_state.try_write() {
                                state.stats.pages_crawled += 1;
                            }
                        }
                        Err(e) => {
                            if let Ok(mut state) = crawler_state.try_write() {
                                state.stats.errors_count += 1;
                                state.logs.push(format!("[ERROR] Zlyhal crawl pre {}: {}", url, e));
                            }
                        }
                    }
                }
            }
        }
    });

    // API a UI Smerovanie
    let app = Router::new()
        .route("/", get(serve_ui))
        .route("/api/status", get(get_status))
        .route("/api/config", post(update_config))
        .route("/api/urls", post(import_urls))
        .route("/api/start", post(start_crawl))
        .route("/api/stop", post(stop_crawl))
        .with_state(default_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5819").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// --- AXUM HANDLERY ---

async fn serve_ui() -> impl IntoResponse {
    Html(include_str!("../index.html"))
}

async fn get_status(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    Json(serde_json::json!({
        "config": s.config,
        "stats": s.stats,
        "is_running": s.is_running,
        "logs": s.logs
    }))
}

async fn update_config(State(state): State<SharedState>, Json(new_cfg): Json<CrawlConfig>) -> impl IntoResponse {
    let mut s = state.write().await;
    s.config = new_cfg;
    s.logs.push("[SYSTEM] Konfigurácia úspešne preložená.".to_string());
    StatusCode::OK
}

async fn import_urls(State(state): State<SharedState>, Json(req): Json<UrlImportRequest>) -> impl IntoResponse {
    let mut s = state.write().await;
    let now = Utc::now();
    for url in req.urls {
        // Manuálny import vždy vloží URL do fronty
        s.visited.insert(url.clone(), now);
        s.queue.push_back((url.clone(), 0));
        s.logs.push(format!("[SYSTEM] Nasilu vložená URL do fronty: {}", url));
    }
    s.stats.pages_queued = s.queue.len();
    StatusCode::OK
}

async fn start_crawl(State(state): State<SharedState>) -> impl IntoResponse {
    let mut s = state.write().await;
    if !s.queue.is_empty() {
        s.is_running = true;
        s.logs.push("[SYSTEM] Crawl úspešne spustený/obnovený.".to_string());
        StatusCode::OK
    } else {
        s.logs.push("[WARN] Nemožno spustiť prázdnu frontu. Najskôr pridaj URL.".to_string());
        StatusCode::BAD_REQUEST
    }
}

async fn stop_crawl(State(state): State<SharedState>) -> impl IntoResponse {
    let mut s = state.write().await;
    s.is_running = false;
    s.logs.push("[SYSTEM] Slučka pozastavená. Zberateľ oddychuje.".to_string());
    StatusCode::OK
}

// --- CRAWL & PARSING CORE ---

async fn crawl_and_index(
    client: &Client,
    url: &str,
    depth: usize,
    cfg: &CrawlConfig,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    // 1. Stiahnutie obsahu
    let res = client.get(url)
        .header("User-Agent", &cfg.user_agent)
        .send()
        .await?
        .text()
        .await?;

    // 2. Parsovanie HTML a extrakcia čistého textu
    let (title, clean_content, new_links) = {
        let doc = ScrapHtml::parse_document(&res);
        
        let title = doc.select(&Selector::parse("title").unwrap())
            .next()
            .map(|e| e.text().collect::<String>())
            .unwrap_or_else(|| "Bez názvu".to_string());

        let mut raw_text_buffer = String::new();
        if let Some(body_el) = doc.select(&Selector::parse("body").unwrap()).next() {
            append_clean_text_nodes(body_el, &mut raw_text_buffer);
        }

        let clean_content: String = raw_text_buffer
            .split_whitespace()
            .collect::<Vec<&str>>()
            .join(" ");

        let mut links = Vec::new();
        if depth < cfg.max_depth {
            for element in doc.select(&Selector::parse("a[href]").unwrap()) {
                if let Some(href) = element.value().attr("href") {
                    if href.starts_with("http") {
                        links.push(href.to_string());
                    }
                }
            }
        }
        (title, clean_content, links)
    };

    // 3. Generovanie unikátneho ID pre OpenSearch (Hash URL adresy)
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let doc_id = hex::encode(hasher.finalize());

    let now = Utc::now();
    let payload = serde_json::json!({
        "url": url,
        "title": title,
        "content": clean_content,
        "indexed_at": now.to_rfc3339()
    });

    // Používame PUT na prepísanie/vytvorenie dokumentu so špecifickým ID
    let index_url = format!("{}/qmay_pages/_doc/{}", cfg.opensearch_url, doc_id);
    let os_res = client.put(&index_url)
        .basic_auth(&cfg.username, Some(&cfg.password))
        .json(&payload)
        .send()
        .await;

    match os_res {
        Ok(_) => {
            if let Ok(mut s) = state.try_write() {
                s.logs.push(format!("[SUCCESS] Aktualizované/Indexované: {} (Dĺžka: {})", url, clean_content.len()));
            }
        }
        Err(e) => {
            if let Ok(mut s) = state.try_write() {
                s.logs.push(format!("[OS-ERROR] Chyba zápisu pre {}: {}", url, e));
            }
        }
    }

    // 4. Spracovanie nových odkazov s TTL (Time-To-Live) logikou
    if depth < cfg.max_depth && !new_links.is_empty() {
        if let Ok(mut s) = state.try_write() {
            for link in new_links {
                let should_crawl = if let Some(last_visited) = s.visited.get(&link) {
                    let duration = now.signed_duration_since(*last_visited);
                    duration.num_days() >= cfg.re_crawl_days
                } else {
                    true // Stránku sme ešte nikdy nevideli
                };

                if should_crawl {
                    s.visited.insert(link.clone(), now);
                    s.queue.push_back((link, depth + 1));
                }
            }
            s.stats.pages_queued = s.queue.len();
        }
    }

    Ok(())
}

/// Rekurzívna pomocná funkcia: Ignoruje <script> a <style>, extrahuje len viditeľný text.
fn append_clean_text_nodes(element: ElementRef, buffer: &mut String) {
    for child in element.children() {
        if let Some(el) = ElementRef::wrap(child) {
            let tag_name = el.value().name();
            if tag_name == "script" || tag_name == "style" || tag_name == "noscript" {
                continue;
            }
            append_clean_text_nodes(el, buffer);
        } else if let Some(text_node) = child.value().as_text() {
            buffer.push_str(text_node);
            buffer.push(' ');
        }
    }
}
