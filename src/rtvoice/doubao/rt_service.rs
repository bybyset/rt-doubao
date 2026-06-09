use std::{
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use crate::rtvoice::{
    doubao::{
        self, DoubaoError,
        config::{API_RESOURCE_ID, AUDIO_CHUNK_SIZE, RuntimeConfig, WS_URL},
        net_client::NetClient,
        protocol::{Message, MsgType},
        request_payloads::{self, SayHelloPayload},
    },
    microphone::{Microphone, MicrophoneReader},
    sound::{PcmFormat, Sound},
};

pub enum RequestType {
    Greeting,
    Audio,
}

struct StartSessionEvent {
    pub dialog_id: String,
}

struct SessionContext {
    session_id: String,
    net_client: Arc<NetClient>,
    dialog_id: Option<String>,

    microphone_running: Arc<AtomicBool>,
    microphone_mod_worker: Option<thread::JoinHandle<()>>,
    text_mod_worker: Option<thread::JoinHandle<()>>,
}

impl SessionContext {
    pub fn new(session_id: String, dialog_id: String, net_client: Arc<NetClient>) -> Self {
        Self {
            session_id,
            net_client,
            dialog_id: Some(dialog_id),
            microphone_running: Arc::new(AtomicBool::new(true)),
            microphone_mod_worker: None,
            text_mod_worker: None,
        }
    }

    pub fn send_greeting(&self) -> Result<(), DoubaoError> {
        println!("发送问候语...");
        let payload = SayHelloPayload {
            content: "你好，我是豆包，有什么可以帮助你的吗？".to_string(),
        };
        let json_payload = serde_json::to_string(&payload)?;
        // event=300 SayHello
        self.net_client
            .send_protocol_message(self.session_id.as_str(), &json_payload, 300)
            .map_err(|e| DoubaoError::NetClient(e))?;
        println!("问候语发送完成");
        Ok(())
    }

    pub fn start_microphone_chat(
        &mut self,
        microphone: Arc<Microphone>,
    ) -> Result<(), DoubaoError> {
        if self.microphone_mod_worker.is_some() {
            return Ok(());
        }
        println!("开始麦克风聊天...");
        let macrophone_reader = microphone
            .open_reader()
            .map_err(|e| DoubaoError::Microphone(e))?;

        let net_client = self.net_client.clone();
        let session_id = self.session_id.clone();
        let running = self.microphone_running.clone();
        let microphone_worker = thread::spawn(move || {
            run_microphone_mode(net_client, session_id, running, macrophone_reader);
        });

        self.microphone_mod_worker = Some(microphone_worker);

        Ok(())
    }

    pub fn stop_microphone_chat(&mut self) {
        self.microphone_running.store(false, Ordering::Relaxed);
        if let Some(worker) = self.microphone_mod_worker.take() {
            let _ = worker.join();
        }
    }

    pub fn send_finish_session(&self) -> Result<(), DoubaoError> {
        println!("发送结束会话...");
        self.net_client
            .send_protocol_message(self.session_id.as_str(), "{}", 102)
            .map_err(|e| DoubaoError::NetClient(e))?;
        println!("会话结束语发送完成");
        Ok(())
    }

    pub fn finish_session(&mut self) {
        self.stop_microphone_chat();
        let _ = self.send_finish_session();
    }

    pub fn close(&mut self) {
        self.finish_session();
        self.dialog_id = None;
    }
}

fn run_microphone_mode(
    net_client: Arc<NetClient>,
    session_id: String,
    running: Arc<AtomicBool>,
    mut macrophone_reader: MicrophoneReader,
) {
    println!("[麦克风] 音频发送线程已启动");
    let mut buffer = [0 as u8; AUDIO_CHUNK_SIZE];
    while running.load(Ordering::Relaxed) {
        // 1. 从麦克风接收所有可用数据
        let read_result = macrophone_reader.read(&mut buffer);
        let read_len = read_result.unwrap_or(0);
        // 2. 发送数据：最多640字节
        if read_len > 0 {
            net_client.send_audio_data(session_id.as_str(), &buffer[..read_len]);
            println!("[麦克风] 已发送 {} 个字节音频数据", read_len);
        }
        thread::sleep(Duration::from_millis(5));
    }
    println!("[麦克风] 音频发送线程已结束");
}
struct ConnectionContext {
    net_client: Arc<NetClient>,
    net_work_thread: thread::JoinHandle<()>,
}

pub struct RtService {
    config: RuntimeConfig,
    sound: Arc<Sound>,
    microphone: Arc<Microphone>,

    session_event_tx: Arc<mpsc::Sender<StartSessionEvent>>,
    session_event_rx: mpsc::Receiver<StartSessionEvent>,

    session_context: Option<SessionContext>,
    connectted: Arc<AtomicBool>,
    connection_context: Option<ConnectionContext>,
}

impl RtService {
    pub fn new(sound: Arc<Sound>, microphone: Arc<Microphone>, config: RuntimeConfig) -> Self {
        // 事件通道
        let (session_event_tx, session_event_rx) = mpsc::channel::<StartSessionEvent>();
        Self {
            sound,
            microphone,
            config,
            session_event_tx: Arc::new(session_event_tx),
            session_event_rx: session_event_rx,

            session_context: None,
            connectted: Arc::new(AtomicBool::new(false)),
            connection_context: None,
        }
    }

    /// 建立websocket连接、发送连接开始消息
    fn start_connection(&mut self, session_id: &str) -> Result<(), DoubaoError> {
        if self.connection_context.is_some() {
            return Ok(());
        }
        let (net_res_msg_tx, net_res_msg_rx) = mpsc::channel::<Message>();
        // 建立wbsocket连接
        let net_client = Arc::new(connect_websocket(
            &session_id,
            &self.config,
            net_res_msg_tx,
        )?);
        // 启动网络工作线程
        let connectted = self.connectted.clone();
        connectted.store(true, Ordering::Relaxed);
        let session_event_tx: Arc<mpsc::Sender<StartSessionEvent>> = self.session_event_tx.clone();
        let sound = self.sound.clone();
        let pcm_format = self.config.pcm_format;
        let net_work_thread = thread::spawn(move || {
            run_net_work_thread(
                connectted,
                net_res_msg_rx,
                session_event_tx,
                sound,
                pcm_format,
            );
        });

        self.connection_context = Some(ConnectionContext {
            net_client,
            net_work_thread,
        });
        // 发送连接开始消息
        self.send_start_connection(session_id)?;
        Ok(())
    }

    /// 启动语音通话服务
    pub fn start(&mut self) -> Result<(), DoubaoError> {
        // 生成会话ID
        let session_id = doubao::protocol::generate_session_id();
        println!("启动语音通话服务，会话ID: {session_id}");

        // 建立连接
        self.start_connection(&session_id)?;

        // 开始会话
        self.start_session(&session_id)?;

        // 监听音频输入
        self.start_microphone_chat()?;

        Ok(())
    }

    fn check_connection(&self) -> Result<Arc<NetClient>, DoubaoError> {
        let net_client = self
            .connection_context
            .as_ref()
            .ok_or_else(|| DoubaoError::NotStartConnection)?
            .net_client
            .clone();
        Ok(net_client)
    }

    fn send_start_connection(&self, session_id: &str) -> Result<(), DoubaoError> {
        println!("发送连接开始消息...");
        let net_client = self.check_connection()?;
        // event=1 StartConnection
        net_client
            .send_protocol_message(session_id, "{}", 1)
            .map_err(|e| DoubaoError::NetClient(e))?;
        println!("连接开始消息发送完成");
        Ok(())
    }

    fn start_session(&mut self, session_id: &str) -> Result<(), DoubaoError> {
        if self.session_context.is_some() {
            return Ok(());
        }
        println!("发送会话开始消息...");
        let mut payload = request_payloads::StartSessionPayload::default();
        payload.tts.audio_config.format = self.config.pcm_format.as_str().to_string();

        // 根据模式设置 dialog.extra.input_mod
        let input_mod = self.config.mode.as_str();
        payload.dialog.extra = request_payloads::create_extra_map(input_mod);
        let json_payload = serde_json::to_string(&payload)?;
        println!("会话开始消息: {}", json_payload);

        // 发送会话开始消息
        // event=100 StartSession
        let net_client = self.check_connection()?;
        net_client
            .send_protocol_message(session_id, &json_payload, 100)
            .map_err(|e| DoubaoError::NetClient(e))?;
        println!("会话开始消息发送完成，等待服务器响应...");
        // 等待服务器返回 event=150 SessionStarted
        let event = self
            .session_event_rx
            .recv_timeout(Duration::from_millis(5000))
            .map_err(|_| DoubaoError::StartSessionTimeout)?;

        // 会话开始完成，更新会话信息
        let session_context =
            SessionContext::new(session_id.to_string(), event.dialog_id, net_client);
        self.session_context = Some(session_context);
        println!("会话开始完成");
        Ok(())
    }

    pub fn start_microphone_chat(&mut self) -> Result<(), DoubaoError> {
        // 检查并获取session_context
        let session_context = self
            .session_context
            .as_mut()
            .ok_or_else(|| DoubaoError::NotStartSession)?;

        // 发送问候消息
        session_context.send_greeting()?;
        // 监听麦克风输入
        session_context.start_microphone_chat(self.microphone.clone())?;
        Ok(())
    }

    pub fn is_net_connected(&self) -> bool {
        self.connectted.load(Ordering::Relaxed)
    }

    pub fn is_session_started(&self) -> bool {
        self.session_context.is_some()
    }

    fn do_finish_session(&mut self) {
        if let Some(mut session_context) = self.session_context.take() {
            let _ = session_context.close();
        }
    }

    fn do_finish_connection(&mut self) {
        // 断开连接
        self.connectted.store(false, Ordering::Relaxed);
        let connection_context = self.connection_context.take();
        if let Some(connection_context) = connection_context {
            connection_context.net_work_thread.join().unwrap();
        }
    }

    pub fn stop(&mut self) {
        self.do_finish_session();
        self.do_finish_connection();
    }
}

fn connect_websocket(
    session_id: &str,
    config: &RuntimeConfig,
    net_res_msg_tx: mpsc::Sender<Message>,
) -> Result<NetClient, DoubaoError> {
    // 连接服务器
    let headers = vec![
        ("X-Api-Resource-Id".to_string(), API_RESOURCE_ID.to_string()),
        ("X-Api-Access-Key".to_string(), config.access_key.clone()),
        ("X-Api-App-Key".to_string(), config.app_key.clone()),
        ("X-Api-App-ID".to_string(), config.app_id.clone()),
        ("X-Api-Connect-Id".to_string(), session_id.to_string()),
    ];

    let callback = move |msg: Message| {
        let _ = net_res_msg_tx.send(msg);
    };

    let net_client =
        NetClient::connect(WS_URL, headers, callback).map_err(|e| DoubaoError::NetClient(e))?;

    Ok(net_client)
}

fn run_net_work_thread(
    connectted: Arc<AtomicBool>,
    net_msg_rx: mpsc::Receiver<Message>,
    session_event_tx: Arc<mpsc::Sender<StartSessionEvent>>,
    sound: Arc<Sound>,
    pcm_format: PcmFormat,
) {
    // 处理服务器响应
    while connectted.load(Ordering::Relaxed) {
        let msg_result = net_msg_rx.recv();
        if let Ok(msg) = msg_result {
            handle_net_response_frame(msg, session_event_tx.clone(), sound.clone(), pcm_format);
        }
    }
}

fn handle_net_response_frame(
    message: Message,
    session_event_tx: Arc<mpsc::Sender<StartSessionEvent>>,
    sound: Arc<Sound>,
    pcm_format: PcmFormat,
) {
    println!("收到服务器消息: {:?}", message.msg_type);
    match message.msg_type {
        MsgType::FullServer => handle_full_server_message(&message, session_event_tx),
        MsgType::AudioOnlyServer => {
            handle_audio_only_server_message(message, sound, pcm_format);
        }
        MsgType::Error => {
            let error_msg = String::from_utf8_lossy(&message.payload);
            eprintln!("收到错误消息 (code={:?}): {}", message.event, error_msg);
        }
        _ => {}
    }
}

fn handle_full_server_message(
    message: &Message,
    session_event_tx: Arc<mpsc::Sender<StartSessionEvent>>,
) {
    let json_str = String::from_utf8_lossy(&message.payload);
    println!(
        "收到服务器完整消息 (event={:?}, session_id={:?}): {}",
        message.event, message.session_id, json_str
    );

    let event = message.event.unwrap_or(0);
    match event {
        50 => {
            println!("成功建立连接");
        }
        51 => {
            println!("建立连接失败");
        }
        52 => {
            println!("连接结束");
        }
        150 => {
            println!("会话已开始");
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&message.payload) {
                if let Some(dialog_id) = v.get("dialog_id").and_then(|x| x.as_str()) {
                    if !dialog_id.is_empty() {
                        // 发送会话启动事件
                        let _ = session_event_tx.send(StartSessionEvent {
                            dialog_id: dialog_id.to_string(),
                        });
                    }
                }
            }
        }
        152 | 153 => {
            println!("会话结束事件");
            //TODO: 处理会话结束事件
        }
        359 => {
            println!("模型一轮音频合成结束事件");
        }
        450 => {
            println!("模型识别出音频流中的首字返回的事件");
        }
        451 => {
            println!("模型识别出用户说话的文本内容");
        }
        _ => {}
    }
}

fn handle_audio_only_server_message(message: Message, sound: Arc<Sound>, pcm_format: PcmFormat) {
    let payload_len = message.payload.len();
    println!(
        "收到音频消息 (event={:?}): session_id={:?}, 数据长度: {}",
        message.event, message.session_id, payload_len
    );
    if payload_len > 0 {
        let audio = message.payload.clone();
        let _ = sound.play(audio, pcm_format);
    }
}
