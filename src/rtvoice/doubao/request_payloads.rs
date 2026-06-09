use std::collections::HashMap;

use serde::Serialize;

use crate::rtvoice::{doubao::config::{DEFAULT_SPEAKER, OUTPUT_SAMPLE_RATE}, sound::PcmFormat};

#[derive(Clone, Debug, Serialize)]
pub struct StartSessionPayload {
    pub asr: AsrPayload,
    pub tts: TtsPayload,
    pub dialog: DialogPayload,
}

impl Default for StartSessionPayload {
    fn default() -> Self {
        Self {
            asr: AsrPayload::default(),
            tts: TtsPayload::default(),
            dialog: DialogPayload::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AsrPayload {
    #[serde(default)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TtsPayload {
    pub speaker: String,
    pub audio_config: AudioConfig,
}

impl Default for TtsPayload {
    fn default() -> Self {
        Self {
            speaker: DEFAULT_SPEAKER.to_string(),
            audio_config: AudioConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AudioConfig {
    pub channel: u32,
    pub format: String,
    pub sample_rate: u32,
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            channel: 1,
            format: PcmFormat::PcmS16le.as_str().to_string(),
            sample_rate: OUTPUT_SAMPLE_RATE,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DialogPayload {
    pub dialog_id: String,
    pub bot_name: String,
    pub system_role: String,
    pub speaking_style: String,
    pub location: LocationInfo,
    #[serde(default)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Default for DialogPayload {
    fn default() -> Self {
        Self {
            dialog_id: String::new(),
            bot_name: "豆包".to_string(),
            system_role: "你使用活泼灵动的女声，性格开朗，热爱生活。".to_string(),
            speaking_style: "你的说话风格简洁明了，语速适中，语调自然。".to_string(),
            location: LocationInfo::default(),
            extra: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LocationInfo {
    pub longitude: f64,
    pub latitude: f64,
    pub city: String,
    pub country: String,
    pub province: String,
    pub district: String,
    pub town: String,
    pub country_code: String,
    pub address: String,
}

impl Default for LocationInfo {
    fn default() -> Self {
        Self {
            longitude: 0.0,
            latitude: 0.0,
            city: "北京".to_string(),
            country: "中国".to_string(),
            province: "北京".to_string(),
            district: String::new(),
            town: String::new(),
            country_code: "CN".to_string(),
            address: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SayHelloPayload {
    pub content: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatTextQueryPayload {
    pub content: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatTtsTextPayload {
    pub start: bool,
    pub end: bool,
    pub content: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatRagTextPayload {
    pub external_rag: String,
}


pub fn create_extra_map(input_mod: &str) -> HashMap<String, serde_json::Value> {
    let mut extra = HashMap::new();
    extra.insert("strict_audit".to_string(), serde_json::Value::Bool(false));
    extra.insert(
        "audit_response".to_string(),
        serde_json::Value::String(
            "抱歉这个问题我无法回答，你可以换个其他话题，我会尽力为你提供帮助。".to_string(),
        ),
    );
    extra.insert(
        "input_mod".to_string(),
        serde_json::Value::String(input_mod.to_string()),
    );
    extra.insert(
        "model".to_string(),
        serde_json::Value::String("O".to_string()),
    );
    extra
}