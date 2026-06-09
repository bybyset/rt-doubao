use crate::rtvoice::sound::PcmFormat;

/// WebSocket 连接地址（与 Java 版本保持一致）。
pub const WS_URL: &str = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue";
/// `X-Api-Resource-Id` 固定值。
pub const API_RESOURCE_ID: &str = "volc.speech.dialog";

/// 麦克风采集采样率（当前 Rust 版本未实现麦克风采集，但保留常量以便对齐协议/配置含义）。
pub const INPUT_SAMPLE_RATE: u32 = 16000;
/// TTS 输出采样率。
pub const OUTPUT_SAMPLE_RATE: u32 = 24000;
/// 单声道。
pub const CHANNELS: u16 = 1;

/// 网络发送音频分片大小（字节），约 20ms 的音频数据。
pub const AUDIO_CHUNK_SIZE: usize = 640;
/// 模拟实时发送间隔（毫秒）。
pub const AUDIO_SEND_INTERVAL_MS: u64 = 20;
/// WAV 文件头大小（字节）。
pub const WAV_HEADER_SIZE: usize = 44;

/// 默认发音人。
pub const DEFAULT_SPEAKER: &str = "zh_female_vv_jupiter_bigtts";

/// 输入模式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputMode {
    Audio,
    Text,
    KeepAlive,
}

impl InputMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Audio => "audio",
            Self::Text => "text",
            Self::KeepAlive => "keep_alive",
        }
    }
}

impl From<&str> for InputMode {
    fn from(s: &str) -> Self {
        match s {
            "audio" => Self::Audio,
            "text" => Self::Text,
            "keep_alive" => Self::KeepAlive,
            _ => panic!("Invalid input mode: {}", s),
        }
    }
}


/// 运行时配置
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// `X-Api-App-ID`
    pub app_id: String,
    /// `X-Api-Access-Key`
    pub access_key: String,
    /// `X-Api-App-Key`
    pub app_key: String,
    /// 输入模式（text/audio）。
    pub mode: InputMode,
    /// TTS 输出 PCM 格式。
    pub pcm_format: PcmFormat,
}

impl RuntimeConfig {
    pub fn new(
        app_id: String,
        access_key: String,
        app_key: String,
        mode: InputMode,
        pcm_format: PcmFormat,
    ) -> Self {
        Self {
            app_id,
            access_key,
            app_key,
            mode,
            pcm_format,
        }
    }

    pub fn new_from_keys(app_id: String, access_key: String, app_key: String) -> Self {
        Self {
            app_id,
            access_key,
            app_key,
            mode: InputMode::Audio,
            pcm_format: PcmFormat::PcmF32le,
        }
    }
}
