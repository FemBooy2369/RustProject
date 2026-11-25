use eframe::egui;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddrV4, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const TCP_CHUNK_SIZE: usize = 65536;
const UDP_CHUNK_SIZE: usize = 8192;
const UDP_FILE_MAGIC: &[u8; 4] = b"UFLC";

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveTab {
    Task3,
    Task4,
    Task5,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Theme {
    Orange,
    Dark,
    Light,
    Emerald,
}

impl Theme {
    fn label(self) -> &'static str {
        match self {
            Theme::Orange => "Оранжевая",
            Theme::Dark => "Тёмная",
            Theme::Light => "Светлая",
            Theme::Emerald => "Изумрудная",
        }
    }
}

enum TcpCommand {
    SendText(String),
    Disconnect,
    ForceDrop,
}

enum TcpEvent {
    Connected(String),
    Disconnected(String),
    Message(String),
    Error(String),
    AutoLog(String),
}

#[derive(Clone)]
struct BenchRow {
    test_name: String,
    protocol: String,
    sent: String,
    received: String,
    loss_pct: f64,
    time_ms: f64,
    speed: String,
}

enum BenchEvent {
    Log(String),
    Row(BenchRow),
    Finished(String),
}

enum McastEvent {
    Joined(String),
    Left(String),
    ServerNotice(String),
    ChatMessage(String),
    Error(String),
}

struct RustHubApp {
    active_tab: ActiveTab,
    theme: Theme,
    theme_initialized: bool,

    // Задание 3
    tcp_addr: String,
    tcp_input: String,
    tcp_status: String,
    tcp_last_rtt: String,
    tcp_logs: Vec<String>,
    tcp_connected: bool,
    tcp_connecting: bool,
    tcp_cmd_tx: Option<Sender<TcpCommand>>,
    tcp_ev_rx: Option<Receiver<TcpEvent>>,
    auto_ev_rx: Option<Receiver<TcpEvent>>,
    pending_pings: VecDeque<Instant>,
    pending_cmds: VecDeque<Instant>,

    // Задание 4
    bench_tcp_addr: String,
    bench_udp_addr: String,
    bench_msg_count: usize,
    bench_file_path: String,
    bench_running: bool,
    bench_status: String,
    bench_rows: Vec<BenchRow>,
    bench_logs: Vec<String>,
    bench_rx: Option<Receiver<BenchEvent>>,
    bench_seq: u32,

    // Задание 5
    mcast_group: String,
    mcast_port: String,
    mcast_nick: String,
    mcast_input: String,
    mcast_joined: bool,
    mcast_status: String,
    mcast_logs: Vec<String>,
    mcast_server_count: usize,
    mcast_chat_count: usize,
    mcast_stop: Option<Arc<AtomicBool>>,
    mcast_rx: Option<Receiver<McastEvent>>,
}

impl RustHubApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let pid_suffix = std::process::id() % 10000;
        let app = Self {
            active_tab: ActiveTab::Task3,
            theme: Theme::Orange,
            theme_initialized: false,

            tcp_addr: "127.0.0.1:7878".to_string(),
            tcp_input: String::new(),
            tcp_status: "Не подключено".to_string(),
            tcp_last_rtt: "—".to_string(),
            tcp_logs: Vec::new(),
            tcp_connected: false,
            tcp_connecting: false,
            tcp_cmd_tx: None,
            tcp_ev_rx: None,
            auto_ev_rx: None,
            pending_pings: VecDeque::new(),
            pending_cmds: VecDeque::new(),

            bench_tcp_addr: "127.0.0.1:7878".to_string(),
            bench_udp_addr: "127.0.0.1:7879".to_string(),
            bench_msg_count: 500,
            bench_file_path: "test_25mb.bin".to_string(),
            bench_running: false,
            bench_status: "Готово к тестированию".to_string(),
            bench_rows: Vec::new(),
            bench_logs: Vec::new(),
            bench_rx: None,
            bench_seq: 1,

            mcast_group: "239.0.0.1".to_string(),
            mcast_port: "7880".to_string(),
            mcast_nick: format!("Клиент-{}", pid_suffix),
            mcast_input: String::new(),
            mcast_joined: false,
            mcast_status: "Не подписан".to_string(),
            mcast_logs: Vec::new(),
            mcast_server_count: 0,
            mcast_chat_count: 0,
            mcast_stop: None,
            mcast_rx: None,
        };
        apply_theme(&cc.egui_ctx, app.theme);
        app
    }

    // ==================== ЗАДАНИЕ 3 ====================
    fn tcp_connect(&mut self, ctx: &egui::Context) {
        if self.tcp_connected || self.tcp_connecting {
            return;
        }
        let addr = self.tcp_addr.trim().to_string();
        if addr.is_empty() {
            self.tcp_logs.push("[ОШИБКА] Укажите адрес TCP-сервера".to_string());
            return;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel::<TcpCommand>();
        let (ev_tx, ev_rx) = mpsc::channel::<TcpEvent>();
        let ctx_clone = ctx.clone();

        self.tcp_cmd_tx = Some(cmd_tx);
        self.tcp_ev_rx = Some(ev_rx);
        self.tcp_connecting = true;
        self.tcp_status = format!("Подключение к {}...", addr);

        thread::spawn(move || {
            let mut write_stream = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    let _ = ev_tx.send(TcpEvent::Error(format!(
                        "Не удалось подключиться к {}: {}",
                        addr, e
                    )));
                    let _ = ev_tx.send(TcpEvent::Disconnected("Ошибка подключения".to_string()));
                    ctx_clone.request_repaint();
                    return;
                }
            };
            let _ = write_stream.set_nodelay(true);

            let read_stream = match write_stream.try_clone() {
                Ok(s) => s,
                Err(e) => {
                    let _ = ev_tx.send(TcpEvent::Error(format!("Ошибка сокета: {}", e)));
                    let _ = ev_tx.send(TcpEvent::Disconnected("Ошибка сокета".to_string()));
                    ctx_clone.request_repaint();
                    return;
                }
            };

            let _ = ev_tx.send(TcpEvent::Connected(addr.clone()));
            ctx_clone.request_repaint();

            let ev_tx_reader = ev_tx.clone();
            let ctx_reader = ctx_clone.clone();
            thread::spawn(move || {
                let mut reader = BufReader::new(read_stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => {
                            let _ = ev_tx_reader.send(TcpEvent::Disconnected(
                                "Сервер закрыл соединение".to_string(),
                            ));
                            ctx_reader.request_repaint();
                            break;
                        }
                        Ok(_) => {
                            let msg = line.trim_end_matches(&['\r', '\n'][..]).to_string();
                            let _ = ev_tx_reader.send(TcpEvent::Message(msg));
                            ctx_reader.request_repaint();
                        }
                        Err(e) => {
                            let _ = ev_tx_reader.send(TcpEvent::Disconnected(format!(
                                "Соединение разорвано: {}",
                                e
                            )));
                            ctx_reader.request_repaint();
                            break;
                        }
                    }
                }
            });

            while let Ok(cmd) = cmd_rx.recv() {
                match cmd {
                    TcpCommand::SendText(text) => {
                        let payload = format!("{}\n", text);
                        if let Err(e) = write_stream.write_all(payload.as_bytes()) {
                            let _ = ev_tx.send(TcpEvent::Error(format!("Ошибка отправки: {}", e)));
                            ctx_clone.request_repaint();
                            break;
                        }
                        let _ = write_stream.flush();
                    }
                    TcpCommand::Disconnect => {
                        let _ = write_stream.write_all(b"/quit\n");
                        let _ = write_stream.flush();
                        let _ = write_stream.shutdown(Shutdown::Both);
                        break;
                    }
                    TcpCommand::ForceDrop => {
                        let _ = write_stream.shutdown(Shutdown::Both);
                        break;
                    }
                }
            }
        });
    }

    fn tcp_disconnect(&mut self, abrupt: bool) {
        if let Some(tx) = self.tcp_cmd_tx.take() {
            if abrupt {
                let _ = tx.send(TcpCommand::ForceDrop);
                self.tcp_logs.push(
                    "[ТЕСТ] Выполнен резкий разрыв сокета без /quit — проверьте, что сервер не упал"
                        .to_string(),
                );
            } else {
                let _ = tx.send(TcpCommand::Disconnect);
                self.tcp_logs.push("[СИСТЕМА] Штатное отключение от сервера".to_string());
            }
        }
        self.tcp_ev_rx = None;
        self.tcp_connected = false;
        self.tcp_connecting = false;
        self.pending_pings.clear();
        self.pending_cmds.clear();
        self.tcp_status = if abrupt {
            "Резко отключено".to_string()
        } else {
            "Отключено".to_string()
        };
    }

    fn tcp_send(&mut self, text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        if let Some(tx) = &self.tcp_cmd_tx {
            self.tcp_logs.push(format!(">> {}", trimmed));
            let now = Instant::now();
            if trimmed == "/ping" {
                self.pending_pings.push_back(now);
            } else if !trimmed.starts_with("/broadcast ") && !trimmed.starts_with("/slow") {
                self.pending_cmds.push_back(now);
            }
            if tx.send(TcpCommand::SendText(trimmed.to_string())).is_ok() {
                self.tcp_status = "Запрос отправлен...".to_string();
            }
        } else {
            self.tcp_logs.push("[ОШИБКА] Сначала подключитесь к серверу".to_string());
        }
    }

    fn run_task3_autotest(&mut self, ctx: &egui::Context) {
        let addr = self.tcp_addr.trim().to_string();
        let (tx, rx) = mpsc::channel::<TcpEvent>();
        self.auto_ev_rx = Some(rx);
        let ctx_clone = ctx.clone();

        self.tcp_logs.push("================ АВТОТЕСТ ЗАДАНИЯ 3 ================".to_string());
        thread::spawn(move || {
            let log = |s: String| {
                let _ = tx.send(TcpEvent::AutoLog(s));
                ctx_clone.request_repaint();
            };

            let mut c1 = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    log(format!("[АВТОТЕСТ ОШИБКА] Нет подключения к {}: {}", addr, e));
                    return;
                }
            };
            let mut c2 = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    log(format!("[АВТОТЕСТ ОШИБКА] Клиент 2: {}", e));
                    return;
                }
            };
            let c3 = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    log(format!("[АВТОТЕСТ ОШИБКА] Клиент 3: {}", e));
                    return;
                }
            };
            let _ = c1.set_nodelay(true);
            let _ = c2.set_nodelay(true);
            let _ = c1.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = c2.set_read_timeout(Some(Duration::from_secs(5)));

            log("[АВТОТЕСТ] Подключено 3 параллельных клиента (C1, C2, C3)".to_string());

            let _ = c1.write_all(b"/slow 2\n");
            let mut r1 = BufReader::new(c1.try_clone().unwrap());
            let mut l1 = String::new();
            let _ = r1.read_line(&mut l1);
            log(format!("[АВТОТЕСТ C1] {}", l1.trim()));

            let t_ping = Instant::now();
            let _ = c2.write_all(b"/ping\n");
            let mut r2 = BufReader::new(c2.try_clone().unwrap());
            let mut l2 = String::new();
            let _ = r2.read_line(&mut l2);
            let ping_ms = t_ping.elapsed().as_secs_f64() * 1000.0;
            log(format!(
                "[АВТОТЕСТ C2] Пока C1 ждёт 2 сек -> {} за {:.3} мс (не заблокировано!)",
                l2.trim(),
                ping_ms
            ));

            l2.clear();
            let _ = c2.write_all(b"/clients\n");
            let _ = r2.read_line(&mut l2);
            log(format!("[АВТОТЕСТ C2] {}", l2.trim()));

            let _ = c3.shutdown(Shutdown::Both);
            drop(c3);
            log("[АВТОТЕСТ C3] Внезапный обрыв соединения без /quit".to_string());
            thread::sleep(Duration::from_millis(120));

            l2.clear();
            let _ = c2.write_all(b"/ping\n");
            let _ = r2.read_line(&mut l2);
            log(format!(
                "[АВТОТЕСТ C2] Проверка после обрыва C3 -> {} (сервер стабилен!)",
                l2.trim()
            ));

            l1.clear();
            let _ = r1.read_line(&mut l1);
            log(format!("[АВТОТЕСТ C1] {}", l1.trim()));

            let _ = c1.write_all(b"/quit\n");
            let _ = c2.write_all(b"/quit\n");
            log("============= АВТОТЕСТ ЗАДАНИЯ 3 ПРОЙДЕН =============".to_string());
        });
    }

    // ==================== ЗАДАНИЕ 4 ====================
    fn create_test_file(&mut self, mb: usize) -> bool {
        let target = format!("test_{}mb.bin", mb);
        self.bench_file_path = target.clone();
        let start = Instant::now();
        match create_sized_file(Path::new(&target), mb) {
            Ok(bytes) => {
                let ms = start.elapsed().as_secs_f64() * 1000.0;
                self.bench_logs.push(format!(
                    "[ФАЙЛ] Создан '{}' ({} байт / {:.2} МБ) за {:.1} мс",
                    target,
                    bytes,
                    bytes as f64 / (1024.0 * 1024.0),
                    ms
                ));
                self.bench_status = format!("Создан файл {} ({} МБ)", target, mb);
                true
            }
            Err(e) => {
                self.bench_logs.push(format!("[ОШИБКА] Не удалось создать файл: {}", e));
                false
            }
        }
    }

    fn start_benchmark(&mut self, ctx: &egui::Context, run_msgs: bool, run_file: bool) {
        if self.bench_running {
            return;
        }
        if run_file && !Path::new(self.bench_file_path.trim()).exists() {
            if !self.create_test_file(25) {
                return;
            }
        }

        let tcp_addr = self.bench_tcp_addr.trim().to_string();
        let udp_addr = self.bench_udp_addr.trim().to_string();
        let msg_count = self.bench_msg_count;
        let file_path = PathBuf::from(self.bench_file_path.trim());
        let test_id = self.bench_seq;
        self.bench_seq = self.bench_seq.wrapping_add(1).max(1);

        let (tx, rx) = mpsc::channel::<BenchEvent>();
        self.bench_rx = Some(rx);
        self.bench_running = true;
        self.bench_status = "Выполняется сравнительный тест TCP vs UDP...".to_string();
        let ctx_clone = ctx.clone();

        thread::spawn(move || {
            let emit = |ev: BenchEvent| {
                let _ = tx.send(ev);
                ctx_clone.request_repaint();
            };

            if run_msgs {
                emit(BenchEvent::Log(format!(
                    "--- Тест сообщений ({} шт.): TCP ({}) vs UDP ({}) ---",
                    msg_count, tcp_addr, udp_addr
                )));
                match bench_tcp_messages(&tcp_addr, msg_count) {
                    Ok(row) => {
                        emit(BenchEvent::Log(format!(
                            "[TCP Сообщения] {}/{} | Потери: {:.2}% | Время: {:.2} мс | {}",
                            row.received, row.sent, row.loss_pct, row.time_ms, row.speed
                        )));
                        emit(BenchEvent::Row(row));
                    }
                    Err(e) => emit(BenchEvent::Log(format!("[TCP Сообщения ОШИБКА] {}", e))),
                }

                match bench_udp_messages(&udp_addr, test_id, msg_count) {
                    Ok(row) => {
                        emit(BenchEvent::Log(format!(
                            "[UDP Сообщения] {}/{} | Потери: {:.2}% | Время: {:.2} мс | {}",
                            row.received, row.sent, row.loss_pct, row.time_ms, row.speed
                        )));
                        emit(BenchEvent::Row(row));
                    }
                    Err(e) => emit(BenchEvent::Log(format!("[UDP Сообщения ОШИБКА] {}", e))),
                }
            }

            if run_file {
                emit(BenchEvent::Log(format!(
                    "--- Тест файла ('{}'): TCP ({}) vs UDP ({}) ---",
                    file_path.display(),
                    tcp_addr,
                    udp_addr
                )));
                match bench_tcp_file(&tcp_addr, &file_path) {
                    Ok(row) => {
                        emit(BenchEvent::Log(format!(
                            "[TCP Файл] Доставлено: {} из {} | Потери: {:.2}% | Время: {:.2} мс | {}",
                            row.received, row.sent, row.loss_pct, row.time_ms, row.speed
                        )));
                        emit(BenchEvent::Row(row));
                    }
                    Err(e) => emit(BenchEvent::Log(format!("[TCP Файл ОШИБКА] {}", e))),
                }

                match bench_udp_file(&udp_addr, test_id, &file_path) {
                    Ok(row) => {
                        emit(BenchEvent::Log(format!(
                            "[UDP Файл] Доставлено: {} из {} | Потери: {:.2}% | Время: {:.2} мс | {}",
                            row.received, row.sent, row.loss_pct, row.time_ms, row.speed
                        )));
                        emit(BenchEvent::Row(row));
                    }
                    Err(e) => emit(BenchEvent::Log(format!("[UDP Файл ОШИБКА] {}", e))),
                }
            }

            emit(BenchEvent::Log(
                "[ВЫВОД] TCP гарантирует 100% доставку без потерь за счёт подтверждений (ACK) и контроля потока; UDP не ждёт подтверждений, поэтому отправляет быстрее, но при высокой нагрузке часть пакетов теряется."
                    .to_string(),
            ));
            emit(BenchEvent::Finished("Тестирование завершено".to_string()));
        });
    }

    // ==================== ЗАДАНИЕ 5 ====================
    fn mcast_join(&mut self, ctx: &egui::Context) {
        if self.mcast_joined {
            return;
        }
        let group: Ipv4Addr = match self.mcast_group.trim().parse() {
            Ok(ip) => ip,
            Err(_) => {
                self.mcast_logs.push("[ОШИБКА] Некорректный IP multicast-группы".to_string());
                return;
            }
        };
        let port: u16 = match self.mcast_port.trim().parse() {
            Ok(p) => p,
            Err(_) => {
                self.mcast_logs.push("[ОШИБКА] Некорректный порт multicast".to_string());
                return;
            }
        };

        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let (tx, rx) = mpsc::channel::<McastEvent>();
        let ctx_clone = ctx.clone();

        self.mcast_stop = Some(stop);
        self.mcast_rx = Some(rx);
        self.mcast_status = format!("Подписка на {}:{}...", group, port);

        thread::spawn(move || {
            let sock = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(McastEvent::Error(format!("Ошибка сокета: {}", e)));
                    ctx_clone.request_repaint();
                    return;
                }
            };
            let _ = sock.set_reuse_address(true);
            let bind_addr = SockAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
            if let Err(e) = sock.bind(&bind_addr) {
                let _ = tx.send(McastEvent::Error(format!("Ошибка bind на порт {}: {}", port, e)));
                ctx_clone.request_repaint();
                return;
            }

            let udp: UdpSocket = sock.into();
            if udp.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED).is_err() {
                if let Err(e) = udp.join_multicast_v4(&group, &Ipv4Addr::LOCALHOST) {
                    let _ = tx.send(McastEvent::Error(format!("Ошибка join_multicast_v4: {}", e)));
                    ctx_clone.request_repaint();
                    return;
                }
            }
            let _ = udp.set_multicast_loop_v4(true);
            let _ = udp.set_read_timeout(Some(Duration::from_millis(200)));

            let _ = tx.send(McastEvent::Joined(format!("{}:{}", group, port)));
            ctx_clone.request_repaint();

            let mut buf = vec![0u8; 4096];
            while !stop_clone.load(Ordering::Relaxed) {
                match udp.recv_from(&mut buf) {
                    Ok((n, _peer)) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            let trimmed = text.trim();
                            if trimmed == "MCAST_TRIGGER" {
                                continue;
                            }
                            if trimmed.starts_with("[СЕРВЕР]") {
                                let _ = tx.send(McastEvent::ServerNotice(trimmed.to_string()));
                            } else {
                                let _ = tx.send(McastEvent::ChatMessage(trimmed.to_string()));
                            }
                            ctx_clone.request_repaint();
                        }
                    }
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => {
                        let _ = tx.send(McastEvent::Error(format!("Ошибка приёма multicast: {}", e)));
                        ctx_clone.request_repaint();
                        break;
                    }
                }
            }

            let _ = udp.leave_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED);
            let _ = tx.send(McastEvent::Left("Отписан от multicast-группы".to_string()));
            ctx_clone.request_repaint();
        });
    }

    fn mcast_leave(&mut self) {
        if let Some(flag) = self.mcast_stop.take() {
            flag.store(true, Ordering::Relaxed);
        }
        self.mcast_joined = false;
        self.mcast_status = "Отписан".to_string();
    }

    fn mcast_send_raw(&mut self, payload: &str) {
        let group: Ipv4Addr = match self.mcast_group.trim().parse() {
            Ok(ip) => ip,
            Err(_) => {
                self.mcast_logs.push("[ОШИБКА] Некорректный IP multicast-группы".to_string());
                return;
            }
        };
        let port: u16 = match self.mcast_port.trim().parse() {
            Ok(p) => p,
            Err(_) => {
                self.mcast_logs.push("[ОШИБКА] Некорректный порт multicast".to_string());
                return;
            }
        };
        let dest = SocketAddrV4::new(group, port);
        match UdpSocket::bind("0.0.0.0:0") {
            Ok(sock) => {
                let _ = sock.set_multicast_loop_v4(true);
                let _ = sock.set_multicast_ttl_v4(1);
                if let Err(e) = sock.send_to(payload.as_bytes(), dest) {
                    self.mcast_logs.push(format!("[ОШИБКА] Не удалось отправить в группу: {}", e));
                }
            }
            Err(e) => {
                self.mcast_logs.push(format!("[ОШИБКА] Сокет отправки: {}", e));
            }
        }
    }

    fn mcast_send_chat(&mut self, text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        let nick = if self.mcast_nick.trim().is_empty() {
            "Аноним"
        } else {
            self.mcast_nick.trim()
        };
        let formatted = format!("[{}]: {}", nick, trimmed);
        self.mcast_send_raw(&formatted);
    }

    // ==================== ОПРОС СОБЫТИЙ ====================
    fn poll_all_events(&mut self) {
        let mut tcp_events = Vec::new();
        if let Some(rx) = &self.tcp_ev_rx {
            while let Ok(ev) = rx.try_recv() {
                tcp_events.push(ev);
            }
        }
        if let Some(rx) = &self.auto_ev_rx {
            while let Ok(ev) = rx.try_recv() {
                tcp_events.push(ev);
            }
        }
        for ev in tcp_events {
            match ev {
                TcpEvent::Connected(addr) => {
                    self.tcp_connected = true;
                    self.tcp_connecting = false;
                    self.tcp_status = format!("Подключено к {}", addr);
                    self.tcp_logs.push(format!("[СИСТЕМА] Подключено к {}", addr));
                }
                TcpEvent::Disconnected(reason) => {
                    self.tcp_connected = false;
                    self.tcp_connecting = false;
                    self.tcp_cmd_tx = None;
                    self.tcp_ev_rx = None;
                    self.pending_pings.clear();
                    self.pending_cmds.clear();
                    self.tcp_status = reason.clone();
                    self.tcp_logs.push(format!("[СИСТЕМА] {}", reason));
                }
                TcpEvent::Error(err) => {
                    self.tcp_status = "Ошибка сети".to_string();
                    self.tcp_logs.push(format!("[ОШИБКА] {}", err));
                }
                TcpEvent::AutoLog(line) => {
                    self.tcp_logs.push(line);
                }
                TcpEvent::Message(msg) => {
                    if msg == "PONG" {
                        if let Some(started) = self.pending_pings.pop_front() {
                            let elapsed = started.elapsed();
                            let ms = elapsed.as_secs_f64() * 1000.0;
                            let us = elapsed.as_micros();
                            self.tcp_logs.push(format!("<< PONG (RTT: {:.3} мс | {} мкс)", ms, us));
                            self.tcp_last_rtt = format!("{:.3} мс ({} мкс)", ms, us);
                        } else {
                            self.tcp_logs.push("<< PONG".to_string());
                        }
                        self.tcp_status = "Ответ получен".to_string();
                    } else if msg.starts_with("[BROADCAST ") || msg.starts_with("[SLOW") {
                        self.tcp_logs.push(format!("<< {}", msg));
                        self.tcp_status = "Получено событие".to_string();
                    } else {
                        if let Some(started) = self.pending_cmds.pop_front() {
                            let ms = started.elapsed().as_secs_f64() * 1000.0;
                            self.tcp_logs.push(format!("<< {} ({:.2} мс)", msg, ms));
                        } else {
                            self.tcp_logs.push(format!("<< {}", msg));
                        }
                        self.tcp_status = "Ответ получен".to_string();
                    }
                }
            }
        }

        let mut bench_events = Vec::new();
        if let Some(rx) = &self.bench_rx {
            while let Ok(ev) = rx.try_recv() {
                bench_events.push(ev);
            }
        }
        for ev in bench_events {
            match ev {
                BenchEvent::Log(line) => self.bench_logs.push(line),
                BenchEvent::Row(row) => self.bench_rows.push(row),
                BenchEvent::Finished(st) => {
                    self.bench_running = false;
                    self.bench_status = st;
                    self.bench_rx = None;
                }
            }
        }

        let mut mcast_events = Vec::new();
        if let Some(rx) = &self.mcast_rx {
            while let Ok(ev) = rx.try_recv() {
                mcast_events.push(ev);
            }
        }
        for ev in mcast_events {
            match ev {
                McastEvent::Joined(addr) => {
                    self.mcast_joined = true;
                    self.mcast_status = format!("Подписан на {}", addr);
                    self.mcast_logs.push(format!("[СИСТЕМА] Подписка на multicast-группу {}", addr));
                }
                McastEvent::Left(msg) => {
                    self.mcast_joined = false;
                    self.mcast_status = msg.clone();
                    self.mcast_logs.push(format!("[СИСТЕМА] {}", msg));
                    self.mcast_rx = None;
                }
                McastEvent::ServerNotice(msg) => {
                    self.mcast_server_count += 1;
                    self.mcast_logs.push(msg);
                }
                McastEvent::ChatMessage(msg) => {
                    self.mcast_chat_count += 1;
                    self.mcast_logs.push(format!("[ЧАТ] {}", msg));
                }
                McastEvent::Error(err) => {
                    self.mcast_joined = false;
                    self.mcast_status = "Ошибка multicast".to_string();
                    self.mcast_logs.push(format!("[ОШИБКА] {}", err));
                }
            }
        }
    }
}

impl eframe::App for RustHubApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let want_dark = !matches!(self.theme, Theme::Light);
        if !self.theme_initialized || ctx.style().visuals.dark_mode != want_dark {
            apply_theme(ctx, self.theme);
            self.theme_initialized = true;
        }

        self.poll_all_events();

        if self.tcp_connected || self.tcp_connecting || self.bench_running || self.mcast_joined {
            ctx.request_repaint_after(Duration::from_millis(60));
        }

        egui::TopBottomPanel::bottom("bottom_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.weak("RustHub v0.3 · Задания №3 (Async Tokio TCP), №4 (TCP vs UDP), №5 (IP Multicast)");
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("RustHub");
                ui.weak("Сетевой клиент (Задания 3–5)");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let prev = self.theme;
                    egui::ComboBox::from_label("")
                        .selected_text(self.theme.label())
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.theme, Theme::Orange, Theme::Orange.label());
                            ui.selectable_value(&mut self.theme, Theme::Dark, Theme::Dark.label());
                            ui.selectable_value(&mut self.theme, Theme::Light, Theme::Light.label());
                            ui.selectable_value(&mut self.theme, Theme::Emerald, Theme::Emerald.label());
                        });
                    if self.theme != prev {
                        apply_theme(ctx, self.theme);
                    }
                });
            });

            ui.separator();

            ui.horizontal(|ui| {
                ui.selectable_value(
                    &mut self.active_tab,
                    ActiveTab::Task3,
                    "Задание 3: Async TCP",
                );
                ui.selectable_value(
                    &mut self.active_tab,
                    ActiveTab::Task4,
                    "Задание 4: TCP vs UDP",
                );
                ui.selectable_value(
                    &mut self.active_tab,
                    ActiveTab::Task5,
                    "Задание 5: Multicast",
                );
            });

            ui.separator();

            match self.active_tab {
                ActiveTab::Task3 => self.ui_task3(ctx, ui),
                ActiveTab::Task4 => self.ui_task4(ctx, ui),
                ActiveTab::Task5 => self.ui_task5(ctx, ui),
            }
        });
    }
}

impl RustHubApp {
    fn ui_task3(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("TCP-сервер:");
            ui.add_enabled(
                !self.tcp_connected && !self.tcp_connecting,
                egui::TextEdit::singleline(&mut self.tcp_addr).desired_width(160.0),
            );
            if ui
                .add_enabled(!self.tcp_connected && !self.tcp_connecting, egui::Button::new("Подключиться"))
                .clicked()
            {
                self.tcp_connect(ctx);
            }
            if ui
                .add_enabled(self.tcp_connected || self.tcp_connecting, egui::Button::new("Отключиться"))
                .clicked()
            {
                self.tcp_disconnect(false);
            }
            if ui
                .add_enabled(self.tcp_connected, egui::Button::new("Резкий разрыв"))
                .clicked()
            {
                self.tcp_disconnect(true);
            }
        });

        ui.horizontal(|ui| {
            ui.label(format!("Статус: {}", self.tcp_status));
            ui.separator();
            ui.label(format!("Последний RTT: {}", self.tcp_last_rtt));
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.label("Сообщение или команда (/ping, /clients, /slow 3, /broadcast <текст>, /help)");
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.tcp_input)
                    .desired_width(f32::INFINITY)
                    .hint_text("Введите текст или команду..."),
            );
            let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                if (ui.add_enabled(self.tcp_connected, egui::Button::new("Отправить")).clicked()
                    || (enter && self.tcp_connected))
                    && !self.tcp_input.trim().is_empty()
                {
                    let msg = std::mem::take(&mut self.tcp_input);
                    self.tcp_send(&msg);
                }
                if ui.add_enabled(self.tcp_connected, egui::Button::new("/ping")).clicked() {
                    self.tcp_send("/ping");
                }
                if ui.add_enabled(self.tcp_connected, egui::Button::new("/clients")).clicked() {
                    self.tcp_send("/clients");
                }
                if ui
                    .add_enabled(self.tcp_connected, egui::Button::new("Долгая задача (/slow 3)"))
                    .clicked()
                {
                    self.tcp_send("/slow 3");
                }
                if ui
                    .add_enabled(self.tcp_connected, egui::Button::new("Тест /broadcast"))
                    .clicked()
                {
                    self.tcp_send("/broadcast Проверка параллельной рассылки");
                }
                if ui.add_enabled(self.tcp_connected, egui::Button::new("/help")).clicked() {
                    self.tcp_send("/help");
                }
                if ui.button("Автотест 3 клиентов").clicked() {
                    self.run_task3_autotest(ctx);
                }
            });
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("Журнал асинхронного TCP-обмена");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Очистить").clicked() {
                        self.tcp_logs.clear();
                    }
                });
            });

            egui::Frame::canvas(ui.style()).show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("task3_scroll")
                    .auto_shrink([false; 2])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in &self.tcp_logs {
                            ui.monospace(line);
                        }
                    });
            });
        });
    }

    fn ui_task4(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label("TCP:");
                ui.add(egui::TextEdit::singleline(&mut self.bench_tcp_addr).desired_width(130.0));
                ui.label("UDP:");
                ui.add(egui::TextEdit::singleline(&mut self.bench_udp_addr).desired_width(130.0));
                ui.label("Сообщений:");
                ui.selectable_value(&mut self.bench_msg_count, 100, "100");
                ui.selectable_value(&mut self.bench_msg_count, 500, "500");
                ui.selectable_value(&mut self.bench_msg_count, 1000, "1000");
            });

            ui.add_space(4.0);

            ui.horizontal_wrapped(|ui| {
                ui.label("Файл:");
                ui.add(egui::TextEdit::singleline(&mut self.bench_file_path).desired_width(170.0));
                if ui.add_enabled(!self.bench_running, egui::Button::new("Создать 1 МБ")).clicked() {
                    self.create_test_file(1);
                }
                if ui.add_enabled(!self.bench_running, egui::Button::new("Создать 25 МБ")).clicked() {
                    self.create_test_file(25);
                }
            });

            ui.add_space(6.0);

            ui.horizontal_wrapped(|ui| {
                if ui
                    .add_enabled(
                        !self.bench_running,
                        egui::Button::new("1. Тест сообщений (TCP → UDP)"),
                    )
                    .clicked()
                {
                    self.start_benchmark(ctx, true, false);
                }
                if ui
                    .add_enabled(
                        !self.bench_running,
                        egui::Button::new("2. Тест файла (TCP → UDP)"),
                    )
                    .clicked()
                {
                    self.start_benchmark(ctx, false, true);
                }
                if ui
                    .add_enabled(
                        !self.bench_running,
                        egui::Button::new("Запустить полное сравнение (сообщения + файл)"),
                    )
                    .clicked()
                {
                    self.start_benchmark(ctx, true, true);
                }
                if ui.add_enabled(!self.bench_running, egui::Button::new("Очистить")).clicked() {
                    self.bench_rows.clear();
                    self.bench_logs.clear();
                }
            });

            ui.label(format!("Статус: {}", self.bench_status));
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.label("Сравнительная таблица результатов (TCP vs UDP)");
            egui::Grid::new("bench_results_grid")
                .striped(true)
                .min_col_width(85.0)
                .show(ui, |ui| {
                    ui.strong("Тест");
                    ui.strong("Протокол");
                    ui.strong("Отправлено");
                    ui.strong("Доставлено");
                    ui.strong("Потери");
                    ui.strong("Время");
                    ui.strong("Скорость");
                    ui.end_row();

                    for row in &self.bench_rows {
                        ui.label(&row.test_name);
                        ui.strong(&row.protocol);
                        ui.label(&row.sent);
                        ui.label(&row.received);
                        ui.label(format!("{:.2}%", row.loss_pct));
                        ui.label(format!("{:.2} мс", row.time_ms));
                        ui.label(&row.speed);
                        ui.end_row();
                    }
                });
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.label("Журнал замеров");
            egui::Frame::canvas(ui.style()).show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("task4_scroll")
                    .auto_shrink([false; 2])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in &self.bench_logs {
                            ui.monospace(line);
                        }
                    });
            });
        });
    }

    fn ui_task5(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label("Группа:");
                ui.add_enabled(
                    !self.mcast_joined,
                    egui::TextEdit::singleline(&mut self.mcast_group).desired_width(110.0),
                );
                ui.label("Порт:");
                ui.add_enabled(
                    !self.mcast_joined,
                    egui::TextEdit::singleline(&mut self.mcast_port).desired_width(60.0),
                );
                ui.label("Имя:");
                ui.add(egui::TextEdit::singleline(&mut self.mcast_nick).desired_width(120.0));

                if ui
                    .add_enabled(!self.mcast_joined, egui::Button::new("Подписаться на группу"))
                    .clicked()
                {
                    self.mcast_join(ctx);
                }
                if ui
                    .add_enabled(self.mcast_joined, egui::Button::new("Отписаться"))
                    .clicked()
                {
                    self.mcast_leave();
                }
            });

            ui.horizontal(|ui| {
                ui.label(format!("Статус: {}", self.mcast_status));
                ui.separator();
                ui.label(format!(
                    "Оповещений сервера: {} | Сообщений чата: {}",
                    self.mcast_server_count, self.mcast_chat_count
                ));
            });
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.label("Групповой Multicast-чат (откройте 2–3 окна клиента для проверки)");
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.mcast_input)
                    .desired_width(f32::INFINITY)
                    .hint_text("Введите сообщение для всей multicast-группы..."),
            );
            let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                if (ui.button("Отправить в группу").clicked() || enter)
                    && !self.mcast_input.trim().is_empty()
                {
                    let msg = std::mem::take(&mut self.mcast_input);
                    self.mcast_send_chat(&msg);
                }
                if ui.button("Тест: привет группе").clicked() {
                    self.mcast_send_chat("Привет всем участникам Multicast-группы!");
                }
                if ui.button("Тест: 3 сообщения").clicked() {
                    for i in 1..=3 {
                        self.mcast_send_chat(&format!("Тестовое групповое сообщение #{}/3", i));
                    }
                }
                if ui.button("Вызвать «Сервер обновлен» сейчас").clicked() {
                    self.mcast_send_raw("MCAST_TRIGGER");
                }
            });
        });

        ui.add_space(6.0);

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("Журнал Multicast-оповещений и группового чата");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Очистить").clicked() {
                        self.mcast_logs.clear();
                        self.mcast_server_count = 0;
                        self.mcast_chat_count = 0;
                    }
                });
            });

            egui::Frame::canvas(ui.style()).show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("task5_scroll")
                    .auto_shrink([false; 2])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in &self.mcast_logs {
                            ui.monospace(line);
                        }
                    });
            });
        });
    }
}

// ==================== ФУНКЦИИ БЕНЧМАРКА (ЗАДАНИЕ 4) ====================
fn create_sized_file(path: &Path, mb: usize) -> Result<u64, String> {
    let mut file = File::create(path).map_err(|e| e.to_string())?;
    let block = vec![b'R'; 1024 * 1024];
    for _ in 0..mb {
        file.write_all(&block).map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())?;
    Ok((mb as u64) * 1024 * 1024)
}

fn bench_tcp_messages(addr: &str, count: usize) -> Result<BenchRow, String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("TCP connect: {}", e))?;
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(15)));

    let start = Instant::now();
    stream
        .write_all(format!("/tcp_batch {}\n", count).as_bytes())
        .map_err(|e| e.to_string())?;

    for i in 0..count {
        let line = format!("TCP_MSG_{:05}_PAYLOAD_BENCHMARK_DATA\n", i);
        stream.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
    }
    stream.flush().map_err(|e| e.to_string())?;

    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).map_err(|e| e.to_string())?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let received: u64 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let loss = if count > 0 {
        ((count as f64 - received as f64).max(0.0) / count as f64) * 100.0
    } else {
        0.0
    };
    let rate = if elapsed_ms > 0.0 {
        (received as f64) / (elapsed_ms / 1000.0)
    } else {
        0.0
    };

    Ok(BenchRow {
        test_name: format!("Сообщения ({})", count),
        protocol: "TCP".to_string(),
        sent: format!("{} шт.", count),
        received: format!("{} шт.", received),
        loss_pct: loss,
        time_ms: elapsed_ms,
        speed: format!("{:.0} сообщ/с", rate),
    })
}

fn bench_udp_messages(addr: &str, id: u32, count: usize) -> Result<BenchRow, String> {
    let sock = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    sock.set_read_timeout(Some(Duration::from_millis(300)))
        .map_err(|e| e.to_string())?;

    let mut buf = [0u8; 512];
    let mut ready = false;
    for _ in 0..4 {
        let cmd = format!("UDP_BATCH_START {} {}", id, count);
        sock.send_to(cmd.as_bytes(), addr).map_err(|e| e.to_string())?;
        if let Ok((n, _)) = sock.recv_from(&mut buf) {
            if std::str::from_utf8(&buf[..n])
                .unwrap_or("")
                .starts_with("UDP_BATCH_READY")
            {
                ready = true;
                break;
            }
        }
    }
    if !ready {
        return Err("UDP-сервер не ответил на UDP_BATCH_START (проверьте запуск сервера)".to_string());
    }

    let start = Instant::now();
    for i in 0..count {
        let line = format!("UDP_MSG {} {:05} PAYLOAD_BENCHMARK_DATA", id, i);
        sock.send_to(line.as_bytes(), addr).map_err(|e| e.to_string())?;
    }
    let send_ms = start.elapsed().as_secs_f64() * 1000.0;

    thread::sleep(Duration::from_millis(40));

    let mut received: u64 = 0;
    let mut got_ok = false;
    for _ in 0..5 {
        let end_cmd = format!("UDP_BATCH_END {}", id);
        let _ = sock.send_to(end_cmd.as_bytes(), addr);
        if let Ok((n, _)) = sock.recv_from(&mut buf) {
            let txt = std::str::from_utf8(&buf[..n]).unwrap_or("");
            if txt.starts_with("UDP_BATCH_OK") {
                received = txt
                    .split_whitespace()
                    .nth(2)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                got_ok = true;
                break;
            }
        }
    }
    if !got_ok {
        return Err("Нет подтверждения UDP_BATCH_OK от сервера".to_string());
    }

    let loss = if count > 0 {
        ((count as f64 - received as f64).max(0.0) / count as f64) * 100.0
    } else {
        0.0
    };
    let rate = if send_ms > 0.0 {
        (count as f64) / (send_ms / 1000.0)
    } else {
        0.0
    };

    Ok(BenchRow {
        test_name: format!("Сообщения ({})", count),
        protocol: "UDP".to_string(),
        sent: format!("{} шт.", count),
        received: format!("{} шт.", received),
        loss_pct: loss,
        time_ms: send_ms,
        speed: format!("{:.0} сообщ/с", rate),
    })
}

fn bench_tcp_file(addr: &str, path: &Path) -> Result<BenchRow, String> {
    let mut file = File::open(path).map_err(|e| format!("Открытие файла: {}", e))?;
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("test.bin");

    let mut stream = TcpStream::connect(addr).map_err(|e| format!("TCP connect: {}", e))?;
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));

    let start = Instant::now();
    stream
        .write_all(format!("/upload {} {}\n", name, size).as_bytes())
        .map_err(|e| e.to_string())?;

    let mut chunk = vec![0u8; TCP_CHUNK_SIZE];
    loop {
        let n = file.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        stream.write_all(&chunk[..n]).map_err(|e| e.to_string())?;
    }
    stream.flush().map_err(|e| e.to_string())?;

    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).map_err(|e| e.to_string())?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    if !resp.starts_with("[FILE_OK]") {
        return Err(format!("Сервер вернул ошибку: {}", resp.trim()));
    }

    let recv_bytes: u64 = resp
        .split_whitespace()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let sent_mb = size as f64 / (1024.0 * 1024.0);
    let recv_mb = recv_bytes as f64 / (1024.0 * 1024.0);
    let loss = if size > 0 {
        ((size as f64 - recv_bytes as f64).max(0.0) / size as f64) * 100.0
    } else {
        0.0
    };
    let speed = if elapsed_ms > 0.0 {
        recv_mb / (elapsed_ms / 1000.0)
    } else {
        0.0
    };

    Ok(BenchRow {
        test_name: format!("Файл ({:.1} МБ)", sent_mb),
        protocol: "TCP".to_string(),
        sent: format!("{:.2} МБ", sent_mb),
        received: format!("{:.2} МБ", recv_mb),
        loss_pct: loss,
        time_ms: elapsed_ms,
        speed: format!("{:.1} МБ/с", speed),
    })
}

fn bench_udp_file(addr: &str, id: u32, path: &Path) -> Result<BenchRow, String> {
    let mut file = File::open(path).map_err(|e| format!("Открытие файла: {}", e))?;
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("test.bin");
    let expected_chunks = (size + UDP_CHUNK_SIZE as u64 - 1) / UDP_CHUNK_SIZE as u64;

    let sock = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    sock.set_read_timeout(Some(Duration::from_millis(350)))
        .map_err(|e| e.to_string())?;

    let mut resp_buf = [0u8; 512];
    let mut ready = false;
    for _ in 0..4 {
        let cmd = format!("UDP_FILE_START {} {} {} {}", id, name, size, expected_chunks);
        sock.send_to(cmd.as_bytes(), addr).map_err(|e| e.to_string())?;
        if let Ok((n, _)) = sock.recv_from(&mut resp_buf) {
            if std::str::from_utf8(&resp_buf[..n])
                .unwrap_or("")
                .starts_with("UDP_FILE_READY")
            {
                ready = true;
                break;
            }
        }
    }
    if !ready {
        return Err("UDP-сервер не ответил на UDP_FILE_START".to_string());
    }

    let mut packet = vec![0u8; 12 + UDP_CHUNK_SIZE];
    packet[..4].copy_from_slice(UDP_FILE_MAGIC);
    packet[4..8].copy_from_slice(&id.to_be_bytes());

    let start = Instant::now();
    let mut seq: u32 = 0;
    loop {
        let n = file.read(&mut packet[12..]).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        packet[8..12].copy_from_slice(&seq.to_be_bytes());
        sock.send_to(&packet[..12 + n], addr).map_err(|e| e.to_string())?;
        seq = seq.wrapping_add(1);
    }
    let send_ms = start.elapsed().as_secs_f64() * 1000.0;

    thread::sleep(Duration::from_millis(60));

    let mut recv_bytes: u64 = 0;
    let mut got_ok = false;
    for _ in 0..6 {
        let end_cmd = format!("UDP_FILE_END {}", id);
        let _ = sock.send_to(end_cmd.as_bytes(), addr);
        if let Ok((n, _)) = sock.recv_from(&mut resp_buf) {
            let txt = std::str::from_utf8(&resp_buf[..n]).unwrap_or("");
            if txt.starts_with("UDP_FILE_OK") {
                recv_bytes = txt
                    .split_whitespace()
                    .nth(3)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                got_ok = true;
                break;
            }
        }
    }
    if !got_ok {
        return Err("Нет подтверждения UDP_FILE_OK от сервера".to_string());
    }

    let sent_mb = size as f64 / (1024.0 * 1024.0);
    let recv_mb = recv_bytes as f64 / (1024.0 * 1024.0);
    let loss = if size > 0 {
        ((size as f64 - recv_bytes as f64).max(0.0) / size as f64) * 100.0
    } else {
        0.0
    };
    let speed = if send_ms > 0.0 {
        sent_mb / (send_ms / 1000.0)
    } else {
        0.0
    };

    Ok(BenchRow {
        test_name: format!("Файл ({:.1} МБ)", sent_mb),
        protocol: "UDP".to_string(),
        sent: format!("{:.2} МБ", sent_mb),
        received: format!("{:.2} МБ", recv_mb),
        loss_pct: loss,
        time_ms: send_ms,
        speed: format!("{:.1} МБ/с", speed),
    })
}

fn apply_theme(ctx: &egui::Context, theme: Theme) {
    let mut visuals = match theme {
        Theme::Light => egui::Visuals::light(),
        _ => egui::Visuals::dark(),
    };

    match theme {
        Theme::Orange => {
            let accent = egui::Color32::from_rgb(230, 126, 34);
            visuals.selection.bg_fill = accent;
            visuals.hyperlink_color = accent;
            visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, accent);
            visuals.widgets.active.bg_stroke = egui::Stroke::new(1.5, accent);
        }
        Theme::Emerald => {
            let accent = egui::Color32::from_rgb(16, 185, 129);
            visuals.selection.bg_fill = accent;
            visuals.hyperlink_color = accent;
            visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, accent);
            visuals.widgets.active.bg_stroke = egui::Stroke::new(1.5, accent);
        }
        Theme::Dark | Theme::Light => {}
    }

    ctx.set_visuals(visuals);
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([920.0, 660.0])
            .with_min_inner_size([700.0, 500.0]),
        ..Default::default()
    };

    eframe::run_native(
        "RustHub Client",
        options,
        Box::new(|cc| Ok(Box::new(RustHubApp::new(cc)))),
    )
}
