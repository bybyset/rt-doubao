use std::fmt::Display;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, atomic::AtomicBool};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use native_tls::{TlsConnector, TlsStream};

use embedded_websocket::{self as ews};
use serde::Serialize;

use crate::rtvoice::doubao::protocol;

use crate::rtvoice::doubao::protocol::{Message, MsgType, ProtocolError};
use crate::rtvoice::doubao::request_payloads::ChatTextQueryPayload;

#[derive(Debug)]
pub enum NetClientError {
    ConnectionFailed(std::io::Error),
    TlsError(native_tls::Error),
    Disconnected(String),
    UrlParseError(url::ParseError),
    UrlSchemeError(String),
    ProtocolError(ProtocolError),
    JsonError(serde_json::Error),
}

impl Display for NetClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub enum NetCommand {
    Bytes(Vec<u8>),
    Disconnect,
}

pub struct NetClient {
    connected: Arc<AtomicBool>,
    work_thread: JoinHandle<()>,
    cmd_sender: mpsc::Sender<NetCommand>,
}

impl NetClient {
    /// 连接服务器
    pub fn connect<C>(
        url: &str,
        headers: Vec<(String, String)>,
        callback: C,
    ) -> Result<Self, NetClientError>
    where
        C: FnMut(Message) + Send + 'static,
    {
        println!("连接服务器: {}", url);
        let ws_url = url::Url::parse(url).map_err(|e| NetClientError::UrlParseError(e))?;
        let scheme = ws_url.scheme();
        let host = ws_url
            .host_str()
            .ok_or_else(|| NetClientError::UrlSchemeError("WS_URL 缺少 host".to_string()))?
            .to_string();
        let port = ws_url
            .port_or_known_default()
            .ok_or_else(|| NetClientError::UrlSchemeError("WS_URL 缺少 port".to_string()))?;

        let mut path = ws_url.path().to_string();
        if let Some(q) = ws_url.query() {
            path.push('?');
            path.push_str(q);
        }

        let origin = match scheme {
            "wss" => format!("https://{host}"),
            "ws" => format!("http://{host}"),
            other => {
                return Err(NetClientError::UrlSchemeError(format!(
                    "不支持的 WS_URL scheme: {other}"
                )));
            }
        };

        let tcp = TcpStream::connect((host.as_str(), port))
            .map_err(|e| NetClientError::ConnectionFailed(e))?;
        tcp.set_nodelay(true).ok();

        let mut stream = match scheme {
            "wss" => {
                let connector = TlsConnector::new().map_err(|e| NetClientError::TlsError(e))?;
                let tls: TlsStream<TcpStream> =
                    connector.connect(host.as_str(), tcp).map_err(|e| match e {
                        native_tls::HandshakeError::Failure(err) => NetClientError::TlsError(err),
                        native_tls::HandshakeError::WouldBlock(_) => {
                            NetClientError::Disconnected("TLS handshake would block".to_string())
                        }
                    })?;
                StdStream::Tls(tls)
            }
            _ => StdStream::Plain(tcp),
        };

        let mut header_lines: Vec<String> = Vec::with_capacity(headers.len());
        for (k, v) in headers {
            header_lines.push(format!("{k}: {v}"));
        }
        let header_refs: Vec<&str> = header_lines.iter().map(|s| s.as_str()).collect();

        println!("请求头: {:?}", header_lines);

        let options = ews::WebSocketOptions {
            path: &path,
            host: &host,
            origin: &origin,
            sub_protocols: None,
            additional_headers: Some(&header_refs),
        };

        // 创建 Framer 并完成握手
        let mut ws_client = ews::WebSocketClient::new_client(ews::EmptyRng::new());
        let mut read_buf = vec![0u8; 8192]; // 8KB
        let mut read_cursor = 0;
        let mut write_buf = vec![0u8; 8192]; // 8KB

        let mut framer = ews::framer::Framer::new(
            &mut read_buf,
            &mut read_cursor,
            &mut write_buf,
            &mut ws_client,
        );

        match framer.connect(&mut stream, &options) {
            Ok(_) => {}
            Err(e) => {
                return Err(NetClientError::Disconnected(format!(
                    "Framer connect error: {e:?}"
                )));
            }
        }

        if ws_client.state != ews::WebSocketState::Open {
            return Err(NetClientError::Disconnected(
                "WebSocket opening handshake 未进入 Open 状态".to_string(),
            ));
        }

        // 创建 channel
        let (send_tx, send_rx) = mpsc::channel::<NetCommand>();

        let connected = Arc::new(AtomicBool::new(true));
        let connected_clone = connected.clone();
        let work_thread = thread::spawn(move || {
            Self::run_work(stream, ws_client, connected_clone, send_rx, callback);
        });

        Ok(Self {
            connected,
            work_thread,
            cmd_sender: send_tx,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn check_connected(&self) -> Result<(), NetClientError> {
        if !self.connected.load(Ordering::Relaxed) {
            return Err(NetClientError::Disconnected("WebSocket 未连接".to_string()));
        }
        Ok(())
    }

    pub fn send_protocol_message(
        &self,
        session_id: &str,
        payload: &str,
        event_id: u32,
    ) -> Result<(), NetClientError> {
        self.check_connected()?;
        let bytes = match event_id {
            1 => protocol::create_start_connection_message()
                .map_err(|e| NetClientError::ProtocolError(e))?,
            100 => protocol::create_start_session_message(session_id, payload)
                .map_err(|e| NetClientError::ProtocolError(e))?,
            _ => {
                let mut msg = Message::new(MsgType::FullClient);
                msg.type_flag = protocol::MSG_TYPE_FLAG_WITH_EVENT;
                msg.event = Some(event_id);
                msg.session_id = Some(session_id.to_string());
                msg.payload = payload.as_bytes().to_vec();
                protocol::marshal(&msg).map_err(|e| NetClientError::ProtocolError(e))?
            }
        };
        self.send_bytes(bytes)
    }

    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), NetClientError> {
        self.cmd_sender
            .send(NetCommand::Bytes(bytes))
            .map_err(|e| NetClientError::Disconnected(e.to_string()))?;
        Ok(())
    }

    pub fn send_audio_data(
        &self,
        session_id: &str,
        audio_data: &[u8],
    ) -> Result<(), NetClientError> {
        self.check_connected()?;
        let bytes = protocol::create_audio_message(session_id, audio_data)
            .map_err(|e| NetClientError::ProtocolError(e))?;
        self.send_bytes(bytes)
    }

    pub fn send_chat_text_query(&self, session_id: &str, text: &str) -> Result<(), NetClientError> {
        self.check_connected()?;

        let payload = ChatTextQueryPayload {
            content: text.to_string(),
        };
        let json_payload =
            serde_json::to_string(&payload).map_err(|e| NetClientError::JsonError(e))?;

        let mut msg = Message::new(MsgType::FullClient);
        msg.type_flag = protocol::MSG_TYPE_FLAG_WITH_EVENT;
        msg.event = Some(501);
        msg.session_id = Some(session_id.to_string());
        msg.payload = json_payload.as_bytes().to_vec();
        let bytes = protocol::marshal(&msg).map_err(|e| NetClientError::ProtocolError(e))?;
        self.send_bytes(bytes)
    }

    pub fn disconnect(&self) -> Result<(), NetClientError> {
        if !self.is_connected() {
            return Ok(());
        }
        self.cmd_sender
            .send(NetCommand::Disconnect)
            .map_err(|e| NetClientError::Disconnected(e.to_string()))?;
        Ok(())
    }

    fn run_work<C>(
        mut stream: StdStream,
        mut ws_client: ews::WebSocketClient<ews::EmptyRng>,
        connected: Arc<AtomicBool>,
        cmd_receiver: mpsc::Receiver<NetCommand>,
        mut callback: C,
    ) -> ()
    where
        C: FnMut(Message) + Send + 'static,
    {
        // 设置 read timeout 为 10ms，避免阻塞读取
        stream.set_read_timeout(Some(Duration::from_millis(10)));

        // 初始化 Framer
        let mut read_buf = vec![0u8; 8192]; // 8KB
        let mut read_cursor = 0;
        let mut write_buf = vec![0u8; 8192]; // 8KB
        let mut framer = ews::framer::Framer::new(
            &mut read_buf,
            &mut read_cursor,
            &mut write_buf,
            &mut ws_client,
        );

        let mut payload_buf = vec![0u8; 262144]; // 256KB
        let mut current_binary = Vec::new();
        while connected.load(Ordering::Relaxed) {
            // 1. 先检查是否有数据要发送（非阻塞）
            while let Ok(msg) = cmd_receiver.try_recv() {
                let data = match msg {
                    NetCommand::Bytes(d) => {
                        println!("NetClient 线程: 准备发送协议消息, {} 字节", d.len());
                        d
                    }
                    NetCommand::Disconnect => {
                        println!("NetClient 线程: 准备发送断开消息");
                        break;
                    }
                };
                // 发送数据
                match framer.write(
                    &mut stream,
                    ews::WebSocketSendMessageType::Binary,
                    true,
                    &data,
                ) {
                    Ok(_) => {
                        println!("NetClient 线程: 数据发送成功");
                    }
                    Err(e) => {
                        eprintln!("Framer write error: {e:?}");
                        connected.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }

            // 2. 尝试读取数据（非阻塞方式，用 try_read 或设置 read timeout）
            // 这里我们用 timeout 的方式，避免阻塞
            match framer.read(&mut stream, &mut payload_buf) {
                Ok(result) => {
                    match result {
                        ews::framer::ReadResult::Binary(data) => {
                            println!("收到二进制数据: {} 字节", data.len());
                            current_binary.extend_from_slice(data);
                            // 尝试循环解析缓冲区中的消息（可能有多条消息）
                            let mut processed_bytes = 0usize;
                            loop {
                                let remaining = &current_binary[processed_bytes..];
                                if remaining.is_empty() {
                                    break;
                                }

                                match protocol::unmarshal(remaining) {
                                    Ok((msg, bytes_consumed)) => {
                                        println!(
                                            "成功解析消息: event={:?}, type={:?}, 消耗字节数={}",
                                            msg.event, msg.msg_type, bytes_consumed
                                        );
                                        // 处理消息
                                        processed_bytes += bytes_consumed;

                                        // 消息回调
                                        callback(msg);
                                    }
                                    Err(e) => {
                                        // 检查是否是"数据长度不足"的错误，如果是则保留缓冲区等待更多数据
                                        match e {
                                            protocol::ProtocolError::InsufficientData
                                            | protocol::ProtocolError::PayloadOverflow
                                            | protocol::ProtocolError::ReadOverflow => {
                                                println!(
                                                    "数据不完整，保留缓冲区等待更多数据 (已处理: {}, 剩余: {} 字节)",
                                                    processed_bytes,
                                                    current_binary.len() - processed_bytes
                                                );
                                            }
                                            _ => {
                                                eprintln!("协议消息解析错误: {e}，清空缓冲区");
                                                processed_bytes = current_binary.len();
                                            }
                                        }
                                        break;
                                    }
                                }
                            }

                            // 从缓冲区中移除已处理的字节
                            if processed_bytes > 0 {
                                current_binary.drain(0..processed_bytes);
                            }
                        }
                        ews::framer::ReadResult::Text(text) => {
                            println!("NetClient 收到文本消息: {text}");
                        }
                        ews::framer::ReadResult::Pong(_) => {
                            println!("NetClient 收到 Pong 消息");
                        }
                        ews::framer::ReadResult::Closed => {
                            println!("NetClient 收到 Close 消息");
                            connected.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                Err(e) => {
                    // 检查是否是 IO 超时
                    match e {
                        ews::framer::FramerError::Io(_) => {
                            // 可能是超时，继续循环
                        }
                        _ => {
                            eprintln!("NetClient Framer read error: {e:?}");
                            connected.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        }
        let _ = framer.close(
            &mut stream,
            ews::WebSocketCloseStatusCode::NormalClosure,
            None,
        );
        println!("NetClient 连接已关闭");
    }
}

enum StdStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl StdStream {
    fn set_read_timeout(&self, duration: Option<Duration>) {
        match self {
            Self::Plain(s) => s.set_read_timeout(duration).ok(),
            Self::Tls(s) => s.get_ref().set_read_timeout(duration).ok(),
        };
    }
}

impl Read for StdStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for StdStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

impl ews::framer::Stream<std::io::Error> for StdStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        Read::read(self, buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        Write::write_all(self, buf)
    }
}
