//! 实时对话 WebSocket 二进制协议编解码。
//!
//! 该模块对应 Java 版本的 `Protocol`：
//! - 前 4 字节为固定头：`version+header_size`、`type+flag`、`serialization+compression`、`reserved`
//! - 后续字段为“条件段”：event/session_id/connect_id/sequence/error_code/payload
//! - 整数均为大端（BE），字符串为 UTF-8，字符串前置 4 字节长度（BE u32）
//! - 音频消息使用 RAW 序列化（`SERIALIZATION_RAW`），其它消息默认 JSON（`SERIALIZATION_JSON`）

use crate::rtvoice::doubao::config::DEFAULT_SPEAKER;
use std::fmt;

#[derive(Debug)]
pub enum ProtocolError {
    InsufficientData,
    PayloadOverflow,
    MissingEvent,
    MissingSessionId,
    MissingConnectId,
    MissingSequence,
    MissingErrorCode,
    ReadOverflow,
    
    Utf8DecodeError(std::str::Utf8Error),
    JsonError(serde_json::Error),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProtocolError::InsufficientData => write!(f, "数据长度不足"),
            ProtocolError::PayloadOverflow => write!(f, "payload 长度越界"),
            ProtocolError::MissingEvent => write!(f, "缺少 event"),
            ProtocolError::MissingSessionId => write!(f, "缺少 session_id"),
            ProtocolError::MissingConnectId => write!(f, "缺少 connect_id"),
            ProtocolError::MissingSequence => write!(f, "缺少 sequence"),
            ProtocolError::MissingErrorCode => write!(f, "缺少 error_code"),
            ProtocolError::ReadOverflow => write!(f, "越界读取"),
            ProtocolError::Utf8DecodeError(e) => write!(f, "utf8 解码失败: {}", e),
            ProtocolError::JsonError(e) => write!(f, "JSON 错误: {}", e),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::str::Utf8Error> for ProtocolError {
    fn from(e: std::str::Utf8Error) -> Self {
        ProtocolError::Utf8DecodeError(e)
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(e: serde_json::Error) -> Self {
        ProtocolError::JsonError(e)
    }
}

/// 消息类型（高 4bit）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    Invalid = 0,
    FullClient = 1,
    AudioOnlyClient = 2,
    FullServer = 9,
    AudioOnlyServer = 11,
    FrontEndResultServer = 12,
    Error = 15,
}

impl MsgType {
    /// 将 4bit 类型值映射为枚举。
    pub fn from_bits(bits: u8) -> Self {
        match bits {
            1 => Self::FullClient,
            2 => Self::AudioOnlyClient,
            9 => Self::FullServer,
            11 => Self::AudioOnlyServer,
            12 => Self::FrontEndResultServer,
            15 => Self::Error,
            _ => Self::Invalid,
        }
    }
}

/// 消息类型标志位（低 4bit）。
pub const MSG_TYPE_FLAG_NO_SEQ: u8 = 0;
pub const MSG_TYPE_FLAG_POSITIVE_SEQ: u8 = 0b1;
pub const MSG_TYPE_FLAG_LAST_NO_SEQ: u8 = 0b10;
pub const MSG_TYPE_FLAG_NEGATIVE_SEQ: u8 = 0b11;
pub const MSG_TYPE_FLAG_WITH_EVENT: u8 = 0b100;

/// 协议版本（与 Go/Java 版本一致）。
pub const VERSION_1: u8 = 0x10;
/// 头部长度单位（4 字节头）。
pub const HEADER_SIZE_4: u8 = 0x1;

/// Raw 二进制序列化（用于音频 payload）。
pub const SERIALIZATION_RAW: u8 = 0;
/// JSON 序列化（用于业务 payload）。
pub const SERIALIZATION_JSON: u8 = 0b1 << 4;
/// 当前实现不启用压缩。
pub const COMPRESSION_NONE: u8 = 0;

/// 解析后的协议消息（将条件字段用 `Option` 表示）。
#[derive(Clone, Debug)]
pub struct Message {
    pub msg_type: MsgType,
    pub type_flag: u8,
    pub event: Option<u32>,
    pub session_id: Option<String>,
    pub connect_id: Option<String>,
    pub sequence: Option<i32>,
    pub error_code: Option<u32>,
    pub payload: Vec<u8>,
}

impl Message {
    /// 创建一个空消息，后续按需填充条件字段与 payload。
    pub fn new(msg_type: MsgType) -> Self {
        Self {
            msg_type,
            type_flag: 0,
            event: None,
            session_id: None,
            connect_id: None,
            sequence: None,
            error_code: None,
            payload: Vec::new(),
        }
    }
}

/// 将消息序列化为二进制帧（默认 JSON 序列化）。
pub fn marshal(msg: &Message) -> Result<Vec<u8>, ProtocolError> {
    marshal_inner(msg, SERIALIZATION_JSON | COMPRESSION_NONE)
}
/// 将二进制帧反序列化为消息结构，返回（消息, 消耗的字节数）。
pub fn unmarshal(data: &[u8]) -> Result<(Message, usize), ProtocolError> {
    if data.len() < 4 {
        return Err(ProtocolError::InsufficientData);
    }

    let mut offset = 0usize;
    let _version_and_header_size = read_u8(data, &mut offset)?;
    let type_and_flag = read_u8(data, &mut offset)?;
    let _serialization_and_compression = read_u8(data, &mut offset)?;
    let _reserved = read_u8(data, &mut offset)?;

    let msg_type_bits = (type_and_flag >> 4) & 0x0f;
    let msg_type = MsgType::from_bits(msg_type_bits);
    let type_flag = type_and_flag & 0x0f;

    let mut msg = Message::new(msg_type);
    msg.type_flag = type_flag;

    if contains_event(type_flag) {
        msg.event = Some(read_u32_be(data, &mut offset)?);
    }

    if should_read_session_id(&msg) {
        msg.session_id = Some(read_string_be(data, &mut offset)?);
    }

    if should_read_connect_id(&msg) {
        msg.connect_id = Some(read_string_be(data, &mut offset)?);
    }

    if contains_sequence(type_flag) {
        msg.sequence = Some(read_i32_be(data, &mut offset)?);
    }

    if msg.msg_type == MsgType::Error {
        msg.error_code = Some(read_u32_be(data, &mut offset)?);
    }

    let payload_len = read_u32_be(data, &mut offset)? as usize;
    if payload_len > 0 {
        if offset + payload_len > data.len() {
            return Err(ProtocolError::PayloadOverflow);
        }
        msg.payload = data[offset..offset + payload_len].to_vec();
    }
    offset += payload_len;

    Ok((msg, offset))
}

fn marshal_inner(msg: &Message, serialization_and_compression: u8) -> Result<Vec<u8>, ProtocolError> {
    let mut out = Vec::with_capacity(4 + msg.payload.len() + 64);

    let version_and_header_size = VERSION_1 | HEADER_SIZE_4;
    out.push(version_and_header_size);

    let type_and_flag = ((msg.msg_type as u8) << 4) | (msg.type_flag & 0x0f);
    out.push(type_and_flag);

    out.push(serialization_and_compression);
    out.push(0);

    if contains_event(msg.type_flag) {
        let event = msg
            .event
            .ok_or_else(|| ProtocolError::MissingEvent)?;
        out.extend_from_slice(&event.to_be_bytes());
    }

    if should_write_session_id(msg) {
        let sid = msg
            .session_id
            .as_ref()
            .ok_or_else(|| ProtocolError::MissingSessionId)?;
        write_string_be(&mut out, sid);
    }

    if should_write_connect_id(msg) {
        let cid = msg
            .connect_id
            .as_ref()
            .ok_or_else(|| ProtocolError::MissingConnectId)?;
        write_string_be(&mut out, cid);
    }

    if contains_sequence(msg.type_flag) {
        let seq = msg
            .sequence
            .ok_or_else(|| ProtocolError::MissingSequence)?;
        out.extend_from_slice(&seq.to_be_bytes());
    }

    if msg.msg_type == MsgType::Error {
        let code = msg
            .error_code
            .ok_or_else(|| ProtocolError::MissingErrorCode)?;
        out.extend_from_slice(&code.to_be_bytes());
    }

    out.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&msg.payload);

    Ok(out)
}

fn contains_event(type_flag: u8) -> bool {
    (type_flag & MSG_TYPE_FLAG_WITH_EVENT) == MSG_TYPE_FLAG_WITH_EVENT
}

fn contains_sequence(type_flag: u8) -> bool {
    (type_flag & MSG_TYPE_FLAG_POSITIVE_SEQ) == MSG_TYPE_FLAG_POSITIVE_SEQ
        || (type_flag & MSG_TYPE_FLAG_NEGATIVE_SEQ) == MSG_TYPE_FLAG_NEGATIVE_SEQ
}

fn should_write_session_id(msg: &Message) -> bool {
    let Some(event) = msg.event else {
        return false;
    };
    // 与 Java/Go 版本逻辑保持一致：某些事件不需要 session_id。
    contains_event(msg.type_flag) && !matches!(event, 1 | 2 | 50 | 51 | 52)
}

fn should_read_session_id(msg: &Message) -> bool {
    let Some(event) = msg.event else {
        return false;
    };
    contains_event(msg.type_flag) && !matches!(event, 1 | 2 | 50 | 51 | 52)
}

fn should_write_connect_id(msg: &Message) -> bool {
    let Some(event) = msg.event else {
        return false;
    };
    // 与 Java/Go 版本逻辑保持一致：connect_id 仅用于连接相关事件。
    contains_event(msg.type_flag) && matches!(event, 50 | 51 | 52)
}

fn should_read_connect_id(msg: &Message) -> bool {
    let Some(event) = msg.event else {
        return false;
    };
    contains_event(msg.type_flag) && matches!(event, 50 | 51 | 52)
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, ProtocolError> {
    if *offset >= data.len() {
        return Err(ProtocolError::ReadOverflow);
    }
    let v = data[*offset];
    *offset += 1;
    Ok(v)
}

fn read_i32_be(data: &[u8], offset: &mut usize) -> Result<i32, ProtocolError> {
    if *offset + 4 > data.len() {
        return Err(ProtocolError::ReadOverflow);
    }
    let b = [
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ];
    *offset += 4;
    Ok(i32::from_be_bytes(b))
}

fn read_u32_be(data: &[u8], offset: &mut usize) -> Result<u32, ProtocolError> {
    if *offset + 4 > data.len() {
        return Err(ProtocolError::ReadOverflow);
    }
    let b = [
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ];
    *offset += 4;
    Ok(u32::from_be_bytes(b))
}

fn read_string_be(data: &[u8], offset: &mut usize) -> Result<String, ProtocolError> {
    let len = read_u32_be(data, offset)? as usize;
    if len == 0 {
        return Ok(String::new());
    }
    if *offset + len > data.len() {
        return Err(ProtocolError::ReadOverflow);
    }
    let bytes = &data[*offset..*offset + len];
    *offset += len;
    let s = std::str::from_utf8(bytes)?;
    Ok(s.to_string())
}

fn write_string_be(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.as_bytes().len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

pub fn create_start_connection_message() -> Result<Vec<u8>, ProtocolError> {
    let mut msg = Message::new(MsgType::FullClient);
    msg.type_flag = MSG_TYPE_FLAG_WITH_EVENT;
    // event=1: StartConnection
    msg.event = Some(1);
    msg.payload = b"{}".to_vec();
    marshal(&msg)
}

pub fn create_start_session_message(
    session_id: &str,
    payload_json: &str,
) -> Result<Vec<u8>, ProtocolError> {
    let mut msg = Message::new(MsgType::FullClient);
    msg.type_flag = MSG_TYPE_FLAG_WITH_EVENT;
    // event=100: StartSession
    msg.event = Some(100);
    msg.session_id = Some(session_id.to_string());
    msg.payload = payload_json.as_bytes().to_vec();
    marshal(&msg)
}

pub fn create_audio_message(session_id: &str, audio_data: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let mut msg = Message::new(MsgType::AudioOnlyClient);
    msg.type_flag = MSG_TYPE_FLAG_WITH_EVENT;
    // event=200: AudioChunk（对齐 Java/Go 版本）。
    msg.event = Some(200);
    msg.session_id = Some(session_id.to_string());
    msg.payload = audio_data.to_vec();
    marshal_raw_audio(&msg)
}

fn marshal_raw_audio(msg: &Message) -> Result<Vec<u8>, ProtocolError> {
    marshal_inner(msg, SERIALIZATION_RAW | COMPRESSION_NONE)
}

pub fn generate_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn create_full_client_message(session_id: &str, text: &str) -> Result<Vec<u8>, ProtocolError> {
    // 兼容旧逻辑：将 text 包裹为 JSON payload。
    let payload = serde_json::json!({
        "session_id": session_id,
        "text": text,
        "speaker": DEFAULT_SPEAKER,
    });

    let mut msg = Message::new(MsgType::FullClient);
    msg.type_flag = MSG_TYPE_FLAG_WITH_EVENT;
    // 保留 event 字段（历史上某些实现要求带 event；此处保持占位为 0）。
    msg.event = Some(0);
    msg.session_id = Some(session_id.to_string());
    msg.payload = serde_json::to_vec(&payload)?;
    marshal(&msg)
}
