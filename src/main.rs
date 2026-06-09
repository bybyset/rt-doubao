mod rtvoice;
use std::{sync::Arc, thread, time::Duration};

use rodio::OutputStream;
use rtvoice::microphone::Microphone;
use rtvoice::sound::Sound;

use rtvoice::doubao::rt_service::RtService;

use crate::rtvoice::{
    doubao::config::{InputMode, RuntimeConfig},
    sound::PcmFormat,
};

use clap::Parser;

fn main() {
    let config = new_config_from_args();
    let (_stream, stream_handle) =
        OutputStream::try_default().unwrap();
    let sound = Sound::start(stream_handle).unwrap();
    let sound_clone = Arc::new(sound);
    let mut microphone = Microphone::start().unwrap();
    let _ = microphone.play();
    let microphone = Arc::new(microphone);

    let mut rt_service = RtService::new(sound_clone, microphone, config);
    let result = rt_service.start();
    if let Err(e) = result {
        eprintln!("Failed to start RT service: {:?}", e);
        return;
    }

    // 等待120秒，确保会话开始
    thread::sleep(Duration::from_secs(120));

    rt_service.stop();
}

fn new_config_from_args() -> RuntimeConfig {
    let args = Args::parse();
    let mut config = RuntimeConfig::new_from_keys(args.app_id, args.access_key, args.app_key);
    let mode = InputMode::from(args.mode.as_str());
    let pcm_format = PcmFormat::from(args.format.as_str());
    config.mode = mode;
    config.pcm_format = pcm_format;
    config
}

#[derive(Parser, Debug)]
#[command(name = "rt-doubao")]
pub struct Args {
    /// 输入模式：`audio` 或 `text` 或 `keep_alive`。
    #[arg(short = 'm', long = "mode", default_value = "keep_alive")]
    pub mode: String,

    /// TTS 输出音频格式：`pcm`（f32le）或 `pcm_s16le`。
    #[arg(short = 'f', long = "format", default_value = "pcm_s16le")]
    pub format: String,

    /// 应用 ID。
    #[arg(long = "app-id", default_value = "your_app_id")]
    pub app_id: String,

    /// 访问密钥。
    #[arg(long = "access-key", default_value = "your_access_key")]
    pub access_key: String,

    /// 应用 Key。
    #[arg(long = "app-key", default_value = "your_app_key")]
    pub app_key: String,
}
