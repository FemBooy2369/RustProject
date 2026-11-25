use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};

type Tx = mpsc::UnboundedSender<String>;
type Clients = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

const TCP_ADDR: &str = "127.0.0.1:7878";
const UDP_ADDR: &str = "127.0.0.1:7879";
const MULTICAST_IP: Ipv4Addr = Ipv4Addr::new(239, 0, 0, 1);
const MULTICAST_PORT: u16 = 7880;
const MAX_FILE_SIZE: u64 = 512 * 1024 * 1024;
const CHUNK_SIZE: usize = 65536;
const UDP_FILE_MAGIC: &[u8; 4] = b"UFLC";

struct UdpBatchState {
    expected: u64,
    received: u64,
    bytes: u64,
    started: Option<Instant>,
    last_seen: Option<Instant>,
}

struct UdpFileState {
    name: String,
    expected_size: u64,
    expected_chunks: u64,
    received_chunks: u64,
    received_bytes: u64,
    started: Option<Instant>,
    last_seen: Option<Instant>,
    writer: Option<std::io::BufWriter<std::fs::File>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let clients: Clients = Arc::new(Mutex::new(HashMap::new()));
    let active_tcp = Arc::new(AtomicUsize::new(0));

    println!("====================================================");
    println!("  RustHub-Server v0.3.0 (Задания 3, 4 и 5)");
    println!("  [Задание 3] Async TCP:  {}", TCP_ADDR);
    println!("  [Задание 4] UDP Bench:  {}", UDP_ADDR);
    println!("  [Задание 5] Multicast:  {}:{}", MULTICAST_IP, MULTICAST_PORT);
    println!("====================================================");

    tokio::spawn(async {
        if let Err(e) = run_udp_server().await {
            eprintln!("[UDP ERR] {}", e);
        }
    });

    let tcp_counter_mcast = Arc::clone(&active_tcp);
    tokio::spawn(async move {
        if let Err(e) = run_multicast_server(tcp_counter_mcast).await {
            eprintln!("[MCAST ERR] {}", e);
        }
    });

    let listener = TcpListener::bind(TCP_ADDR).await?;
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let _ = stream.set_nodelay(true);
                let clients_clone = Arc::clone(&clients);
                let counter_clone = Arc::clone(&active_tcp);
                tokio::spawn(async move {
                    handle_tcp_client(stream, addr, clients_clone, counter_clone).await;
                });
            }
            Err(e) => {
                eprintln!("[TCP ERR] Ошибка accept: {}", e);
            }
        }
    }
}

// Задание 3: асинхронная обработка TCP-клиентов и защита от разрывов
async fn handle_tcp_client(
    stream: TcpStream,
    addr: SocketAddr,
    clients: Clients,
    active_tcp: Arc<AtomicUsize>,
) {
    let count = active_tcp.fetch_add(1, Ordering::SeqCst) + 1;
    println!("[TCP +] Подключился клиент: {} (активных: {})", addr, count);

    let (reader, mut writer) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    {
        let mut map = clients.lock().await;
        map.insert(addr, tx.clone());
    }

    let writer_task = tokio::spawn(async move {
        while let Some(mut msg) = rx.recv().await {
            if !msg.ends_with('\n') {
                msg.push('\n');
            }
            if writer.write_all(msg.as_bytes()).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                println!("[TCP] Клиент {} закрыл соединение (EOF)", addr);
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if trimmed == "/ping" {
                    let _ = tx.send("PONG".to_string());
                } else if trimmed == "/clients" {
                    let map = clients.lock().await;
                    let mut list: Vec<String> = map.keys().map(|a| a.to_string()).collect();
                    list.sort();
                    let _ = tx.send(format!(
                        "Активных клиентов ({}): {}",
                        list.len(),
                        list.join(", ")
                    ));
                } else if trimmed == "/help" {
                    let _ = tx.send(
                        "Команды: /ping | /clients | /slow <сек> | /broadcast <текст> | /upload <имя> <размер> | /quit | текст -> ВЕРХНИЙ РЕГИСТР".to_string(),
                    );
                } else if trimmed == "/slow" || trimmed.starts_with("/slow ") {
                    let secs: u64 = trimmed
                        .strip_prefix("/slow")
                        .unwrap_or("")
                        .trim()
                        .parse()
                        .unwrap_or(3)
                        .clamp(1, 10);
                    let _ = tx.send(format!(
                        "[SLOW] Задача на {} сек запущена асинхронно (другие клиенты не заблокированы)",
                        secs
                    ));
                    let tx_slow = tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(secs)).await;
                        let _ = tx_slow.send(format!(
                            "[SLOW_DONE] Асинхронная задача на {} сек завершена",
                            secs
                        ));
                    });
                } else if trimmed == "/broadcast" {
                    let _ = tx.send(
                        "Ошибка: укажите текст после /broadcast (например: /broadcast привет)"
                            .to_string(),
                    );
                } else if let Some(rest) = trimmed.strip_prefix("/broadcast ") {
                    let msg = rest.trim();
                    if msg.is_empty() {
                        let _ = tx.send(
                            "Ошибка: сообщение для /broadcast не может быть пустым".to_string(),
                        );
                    } else {
                        let out = format!("[BROADCAST от {}]: {}", addr, msg.to_uppercase());
                        let map = clients.lock().await;
                        for client_tx in map.values() {
                            let _ = client_tx.send(out.clone());
                        }
                    }
                } else if let Some(rest) = trimmed.strip_prefix("/tcp_batch ") {
                    let count: u64 = rest.trim().parse().unwrap_or(0);
                    if !handle_tcp_batch(&mut buf_reader, count, addr, &tx).await {
                        break;
                    }
                } else if let Some(rest) = trimmed.strip_prefix("/upload ") {
                    if !receive_tcp_file(&mut buf_reader, rest, addr, &tx).await {
                        break;
                    }
                } else if trimmed == "/quit" {
                    let _ = tx.send("Отключение по команде /quit".to_string());
                    break;
                } else {
                    let _ = tx.send(trimmed.to_uppercase());
                }
            }
            Err(e) => {
                println!("[TCP !] Внезапный обрыв клиента {} (сервер продолжает работу): {}", addr, e);
                break;
            }
        }
    }

    {
        let mut map = clients.lock().await;
        map.remove(&addr);
    }

    let left = active_tcp.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
    drop(tx);
    let _ = writer_task.await;
    println!("[TCP -] Клиент {} отключен (активных: {})", addr, left);
}

async fn handle_tcp_batch(
    buf_reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    expected: u64,
    addr: SocketAddr,
    tx: &Tx,
) -> bool {
    let start = Instant::now();
    let mut received: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut msg = String::new();

    for _ in 0..expected {
        msg.clear();
        match buf_reader.read_line(&mut msg).await {
            Ok(0) => return false,
            Ok(n) => {
                received += 1;
                total_bytes += n as u64;
            }
            Err(_) => return false,
        }
    }

    let ms = start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "[TCP BENCH] От {}: принято {}/{} сообщений ({} байт) за {:.2} мс",
        addr, received, expected, total_bytes, ms
    );
    let _ = tx.send(format!("[TCP_BATCH_OK] {} {} {:.2}", received, total_bytes, ms));
    true
}

async fn receive_tcp_file(
    buf_reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    args: &str,
    addr: SocketAddr,
    tx: &Tx,
) -> bool {
    let mut parts = args.rsplitn(2, ' ');
    let size_str = match parts.next() {
        Some(s) => s.trim(),
        None => {
            let _ = tx.send("[FILE_ERR] Формат: /upload <имя> <размер>".to_string());
            return true;
        }
    };
    let raw_name = match parts.next() {
        Some(n) => n.trim(),
        None => {
            let _ = tx.send("[FILE_ERR] Формат: /upload <имя> <размер>".to_string());
            return true;
        }
    };

    let size: u64 = match size_str.parse() {
        Ok(s) if s <= MAX_FILE_SIZE => s,
        Ok(_) => {
            let _ = tx.send("[FILE_ERR] Превышен лимит размера файла".to_string());
            return false;
        }
        Err(_) => {
            let _ = tx.send("[FILE_ERR] Некорректный размер файла".to_string());
            return true;
        }
    };

    let safe_name = Path::new(raw_name)
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| !n.is_empty())
        .unwrap_or("upload.bin");

    if let Err(e) = fs::create_dir_all("uploads").await {
        let _ = tx.send(format!("[FILE_ERR] Ошибка создания uploads: {}", e));
        return false;
    }

    let save_path: PathBuf = Path::new("uploads").join(format!("tcp_{}", safe_name));
    let mut file = match fs::File::create(&save_path).await {
        Ok(f) => f,
        Err(e) => {
            let _ = tx.send(format!("[FILE_ERR] Не удалось создать файл: {}", e));
            return false;
        }
    };

    let start = Instant::now();
    let mut remaining = size;
    let mut chunk = vec![0u8; CHUNK_SIZE];

    while remaining > 0 {
        let to_read = (remaining as usize).min(CHUNK_SIZE);
        match buf_reader.read(&mut chunk[..to_read]).await {
            Ok(0) => {
                println!("[TCP FILE !] Обрыв при приёме файла от {}", addr);
                return false;
            }
            Ok(n) => {
                if let Err(e) = file.write_all(&chunk[..n]).await {
                    let _ = tx.send(format!("[FILE_ERR] Ошибка записи файла: {}", e));
                    return false;
                }
                remaining -= n as u64;
            }
            Err(e) => {
                println!("[TCP FILE !] Ошибка сокета от {}: {}", addr, e);
                return false;
            }
        }
    }

    if let Err(e) = file.flush().await {
        let _ = tx.send(format!("[FILE_ERR] Ошибка сохранения файла: {}", e));
        return false;
    }

    let secs = start.elapsed().as_secs_f64();
    let ms = secs * 1000.0;
    let mb = size as f64 / (1024.0 * 1024.0);
    let speed = if secs > 0.0 { mb / secs } else { 0.0 };

    println!(
        "[TCP FILE] Принят '{}' ({} байт / {:.2} МБ) от {} за {:.2} мс ({:.1} МБ/с)",
        safe_name, size, mb, addr, ms, speed
    );

    let _ = tx.send(format!(
        "[FILE_OK] {} {} {:.2} {:.1}",
        safe_name, size, ms, speed
    ));

    true
}

// Задание 4: UDP-сервер для замера скорости и потерь сообщений и файлов
async fn run_udp_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = UdpSocket::bind(UDP_ADDR).await?;
    let mut buf = vec![0u8; 65535];
    let mut batches: HashMap<(SocketAddr, u32), UdpBatchState> = HashMap::new();
    let mut files: HashMap<(SocketAddr, u32), UdpFileState> = HashMap::new();

    loop {
        let (n, peer) = socket.recv_from(&mut buf).await?;

        if n >= 12 && &buf[..4] == UDP_FILE_MAGIC {
            let id = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let payload = &buf[12..n];
            if let Some(state) = files.get_mut(&(peer, id)) {
                let now = Instant::now();
                if state.started.is_none() {
                    state.started = Some(now);
                }
                state.last_seen = Some(now);
                state.received_chunks += 1;
                state.received_bytes += payload.len() as u64;
                if let Some(w) = state.writer.as_mut() {
                    let _ = std::io::Write::write_all(w, payload);
                }
            }
            continue;
        }

        let text = match std::str::from_utf8(&buf[..n]) {
            Ok(s) => s.trim(),
            Err(_) => continue,
        };

        if text == "UDP_PING" {
            let _ = socket.send_to(b"UDP_PONG", peer).await;
        } else if let Some(rest) = text.strip_prefix("UDP_BATCH_START ") {
            let mut it = rest.split_whitespace();
            if let (Some(id_s), Some(exp_s)) = (it.next(), it.next()) {
                if let (Ok(id), Ok(expected)) = (id_s.parse::<u32>(), exp_s.parse::<u64>()) {
                    batches.insert(
                        (peer, id),
                        UdpBatchState {
                            expected,
                            received: 0,
                            bytes: 0,
                            started: None,
                            last_seen: None,
                        },
                    );
                    let reply = format!("UDP_BATCH_READY {}", id);
                    let _ = socket.send_to(reply.as_bytes(), peer).await;
                }
            }
        } else if let Some(rest) = text.strip_prefix("UDP_MSG ") {
            let mut it = rest.split_whitespace();
            if let Some(id_s) = it.next() {
                if let Ok(id) = id_s.parse::<u32>() {
                    if let Some(state) = batches.get_mut(&(peer, id)) {
                        let now = Instant::now();
                        if state.started.is_none() {
                            state.started = Some(now);
                        }
                        state.last_seen = Some(now);
                        state.received += 1;
                        state.bytes += n as u64;
                    }
                }
            }
        } else if let Some(rest) = text.strip_prefix("UDP_BATCH_END ") {
            if let Ok(id) = rest.trim().parse::<u32>() {
                if let Some(state) = batches.get(&(peer, id)) {
                    let ms = match (state.started, state.last_seen) {
                        (Some(s), Some(e)) => e.saturating_duration_since(s).as_secs_f64() * 1000.0,
                        _ => 0.0,
                    };
                    println!(
                        "[UDP BENCH] От {}: принято {}/{} сообщений ({} байт) за {:.2} мс",
                        peer, state.received, state.expected, state.bytes, ms
                    );
                    let reply = format!(
                        "UDP_BATCH_OK {} {} {} {:.2}",
                        id, state.received, state.bytes, ms
                    );
                    let _ = socket.send_to(reply.as_bytes(), peer).await;
                }
            }
        } else if let Some(rest) = text.strip_prefix("UDP_FILE_START ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() >= 4 {
                if let (Ok(id), Ok(expected_size), Ok(expected_chunks)) = (
                    parts[0].parse::<u32>(),
                    parts[2].parse::<u64>(),
                    parts[3].parse::<u64>(),
                ) {
                    let safe_name = Path::new(parts[1])
                        .file_name()
                        .and_then(|s| s.to_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("upload.bin")
                        .to_string();

                    let _ = std::fs::create_dir_all("uploads");
                    let save_path = Path::new("uploads").join(format!("udp_{}", safe_name));
                    let writer = std::fs::File::create(&save_path)
                        .ok()
                        .map(std::io::BufWriter::new);

                    files.insert(
                        (peer, id),
                        UdpFileState {
                            name: safe_name,
                            expected_size,
                            expected_chunks,
                            received_chunks: 0,
                            received_bytes: 0,
                            started: None,
                            last_seen: None,
                            writer,
                        },
                    );
                    let reply = format!("UDP_FILE_READY {}", id);
                    let _ = socket.send_to(reply.as_bytes(), peer).await;
                }
            }
        } else if let Some(rest) = text.strip_prefix("UDP_FILE_END ") {
            if let Ok(id) = rest.trim().parse::<u32>() {
                if let Some(state) = files.get_mut(&(peer, id)) {
                    if let Some(mut w) = state.writer.take() {
                        let _ = std::io::Write::flush(&mut w);
                    }
                    let ms = match (state.started, state.last_seen) {
                        (Some(s), Some(e)) => e.saturating_duration_since(s).as_secs_f64() * 1000.0,
                        _ => 0.0,
                    };
                    println!(
                        "[UDP FILE] От {}: файл '{}' — чанков {}/{}, байт {}/{} за {:.2} мс",
                        peer,
                        state.name,
                        state.received_chunks,
                        state.expected_chunks,
                        state.received_bytes,
                        state.expected_size,
                        ms
                    );
                    let reply = format!(
                        "UDP_FILE_OK {} {} {} {:.2}",
                        id, state.received_chunks, state.received_bytes, ms
                    );
                    let _ = socket.send_to(reply.as_bytes(), peer).await;
                }
            }
        }
    }
}

// Задание 5: Multicast-сервер (периодические оповещения и лог группового чата)
async fn run_multicast_server(
    active_tcp: Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mcast_dest = SocketAddrV4::new(MULTICAST_IP, MULTICAST_PORT);

    let sender = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    let _ = sender.set_multicast_loop_v4(true);
    let _ = sender.set_multicast_ttl_v4(1);

    let seq = Arc::new(AtomicUsize::new(0));

    let sender_periodic = Arc::clone(&sender);
    let seq_periodic = Arc::clone(&seq);
    let tcp_periodic = Arc::clone(&active_tcp);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let n = seq_periodic.fetch_add(1, Ordering::SeqCst) + 1;
            let tcp_cnt = tcp_periodic.load(Ordering::SeqCst);
            let msg = format!(
                "[СЕРВЕР] Сервер обновлен (оповещение #{}, активных TCP: {})",
                n, tcp_cnt
            );
            if let Err(e) = sender_periodic.send_to(msg.as_bytes(), mcast_dest).await {
                eprintln!("[MCAST SEND ERR] {}", e);
            } else {
                println!("[MCAST ->] {}", msg);
            }
        }
    });

    let recv_sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    recv_sock.set_reuse_address(true)?;
    let bind_addr = SockAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MULTICAST_PORT));
    recv_sock.bind(&bind_addr)?;
    recv_sock.set_nonblocking(true)?;
    let std_udp: std::net::UdpSocket = recv_sock.into();
    if std_udp
        .join_multicast_v4(&MULTICAST_IP, &Ipv4Addr::UNSPECIFIED)
        .is_err()
    {
        let _ = std_udp.join_multicast_v4(&MULTICAST_IP, &Ipv4Addr::LOCALHOST);
    }

    let listener = UdpSocket::from_std(std_udp)?;
    let mut buf = vec![0u8; 4096];

    loop {
        let (n, peer) = listener.recv_from(&mut buf).await?;
        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
            let trimmed = text.trim();
            if trimmed == "MCAST_TRIGGER" {
                let num = seq.fetch_add(1, Ordering::SeqCst) + 1;
                let tcp_cnt = active_tcp.load(Ordering::SeqCst);
                let msg = format!(
                    "[СЕРВЕР] Сервер обновлен (внеплановое оповещение #{}, активных TCP: {})",
                    num, tcp_cnt
                );
                let _ = sender.send_to(msg.as_bytes(), mcast_dest).await;
                println!("[MCAST ->] {}", msg);
            } else if !trimmed.starts_with("[СЕРВЕР]") {
                println!("[MCAST CHAT <- {}] {}", peer, trimmed);
            }
        }
    }
}
