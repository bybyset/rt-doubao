use std::fmt::Display;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Stream, SupportedStreamConfig};

#[derive(Debug)]
pub enum MicrophoneError {
    Audio(String),
    Device(String),
    Play(cpal::PlayStreamError),
    Pause(cpal::PauseStreamError),
    UnsupportedConfig(cpal::SupportedStreamConfigsError),
    Unknown(String),
}

impl Display for MicrophoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Audio(e) => write!(f, "audio error: {e}"),
            Self::Device(e) => write!(f, "device error: {e}"),
            Self::Play(e) => write!(f, "play error: {e}"),
            Self::Pause(e) => write!(f, "pause error: {e}"),
            Self::UnsupportedConfig(e) => write!(f, "unsupported config error: {e}"),
            Self::Unknown(e) => write!(f, "unknown error: {e}"),
        }
    }
}

type DataConsumer = Box<dyn Fn(Vec<u8>) + Send>;

pub struct Microphone {
    worker_thread: thread::JoinHandle<()>,
    stream: Stream,
    data_consumers: Arc<Mutex<Vec<DataConsumer>>>,
    running: Arc<AtomicBool>,
}

impl Microphone {
    pub fn start() -> Result<Self, MicrophoneError> {
        let host = cpal::default_host();
        let device = host
            .default_input_device()
            .ok_or_else(|| MicrophoneError::Device("未找到默认输入设备".to_string()))?;
        let supported_configs: Vec<_> = device
            .supported_input_configs()
            .map_err(|e: cpal::SupportedStreamConfigsError| MicrophoneError::UnsupportedConfig(e))?
            .collect();

        // 打印支持的输入配置
        for supported_config in &supported_configs {
            println!("支持的输入配置: {:?}", supported_config);
        }

        let default_config = device
            .default_input_config()
            .map_err(|e| MicrophoneError::Device(e.to_string()))?;
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        let stream = device_stream(&device, &default_config, tx)?;
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let data_consumers = Arc::new(Mutex::new(Vec::new()));
        let data_consumers_clone = data_consumers.clone();

        let worker_thread = thread::spawn(move || {
            Self::run_worker(running_clone, rx, data_consumers_clone).unwrap();
        });

        Ok(Self {
            worker_thread,
            stream,
            data_consumers,
            running,
        })
    }

    pub fn play(&mut self) -> Result<(), MicrophoneError> {
        self.stream.play().map_err(|e| MicrophoneError::Play(e))?;
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), MicrophoneError> {
        self.stream.pause().map_err(|e| MicrophoneError::Pause(e))?;
        Ok(())
    }

    fn register_data_consumer(&self, consumer: DataConsumer) -> Result<usize, MicrophoneError> {
        let mut audio_consumers = self
            .data_consumers
            .lock()
            .map_err(|e| MicrophoneError::Unknown(e.to_string()))?;
        audio_consumers.push(consumer);
        Ok(audio_consumers.len() - 1)
    }
    fn unregister_data_consumer(&self, index: usize) -> Result<DataConsumer, MicrophoneError> {
        do_unregister_data_consumer(self.data_consumers.clone(), index)
    }

    pub fn open_reader(&self) -> Result<MicrophoneReader, MicrophoneError> {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let data_consumer = Box::new(move |bytes: Vec<u8>| {
            if let Err(e) = tx.send(bytes) {
                eprintln!("[麦克风] 发送音频数据失败: {}", e);
            }
        });
        let consumer_index = self.register_data_consumer(data_consumer)?;
        let data_consumers_clone = self.data_consumers.clone();
        let on_close = Box::new(move || {
            let _ = do_unregister_data_consumer(data_consumers_clone, consumer_index);
        });
        let reader = MicrophoneReader::new(on_close, rx);

        Ok(reader)
    }

    fn run_worker(
        running: Arc<AtomicBool>,
        rx_consumer: mpsc::Receiver<Vec<u8>>,
        data_consumers: Arc<Mutex<Vec<DataConsumer>>>,
    ) -> Result<(), MicrophoneError> {
        while running.load(Ordering::Relaxed) {
            if let Ok(bytes) = rx_consumer.recv_timeout(std::time::Duration::from_millis(5)) {
                let audio_consumers = data_consumers.lock().unwrap();
                for consumer in audio_consumers.iter() {
                    consumer(bytes.clone());
                }
            }
        }
        running.store(false, Ordering::Relaxed);
        println!("[麦克风]线程结束");
        Ok(())
    }
}

fn do_unregister_data_consumer(
    audio_consumers: Arc<Mutex<Vec<DataConsumer>>>,
    index: usize,
) -> Result<DataConsumer, MicrophoneError> {
    let mut audio_consumers = audio_consumers
        .lock()
        .map_err(|e| MicrophoneError::Unknown(e.to_string()))?;
    let consumer = audio_consumers.remove(index);
    Ok(consumer)
}

fn device_stream(
    device: &Device,
    stream_config: &SupportedStreamConfig,
    tx_product: mpsc::Sender<Vec<u8>>,
) -> Result<Stream, MicrophoneError> {
    let err_fn = |err| eprintln!("音频流错误: {}", err);
    let config = stream_config.config();
    let channels = config.channels;
    let sample_rate = config.sample_rate.0;
    let sample_format = stream_config.sample_format();
    let stream = match sample_format {
        cpal::SampleFormat::I16 => device
            .build_input_stream(
                &config,
                move |data: &[i16], _: &cpal::InputCallbackInfo| {
                    // Downmix to mono
                    let mono_samples = downmix_to_mono_i16(data, channels);
                    // Resample to 16kHz
                    let resampled = resample_i16(&mono_samples, sample_rate);
                    let bytes = i16_samples_to_bytes_le(&resampled);
                    if let Err(e) = tx_product.send(bytes.clone()) {
                        eprintln!("[麦克风] 发送音频数据失败: {}", e);
                    }
                },
                err_fn,
                None,
            )
            .map_err(|e| MicrophoneError::Audio(e.to_string()))?,
        cpal::SampleFormat::F32 => {
            device
                .build_input_stream(
                    &config,
                    move |data: &[f32], _: &cpal::InputCallbackInfo| {
                        // Downmix to mono
                        let mono_samples = downmix_to_mono_f32(data, channels);
                        let mut i16_samples = Vec::with_capacity(mono_samples.len());
                        for s in mono_samples {
                            let scaled = (s * 32767.0).round();
                            let clamped = scaled.clamp(-32768.0, 32767.0) as i16;
                            i16_samples.push(clamped);
                        }
                        // Resample to 16kHz
                        let resampled = resample_i16(&i16_samples, sample_rate);
                        let bytes = i16_samples_to_bytes_le(&resampled);
                        if let Err(e) = tx_product.send(bytes.clone()) {
                            eprintln!("[麦克风] 发送音频数据失败: {}", e);
                        }
                    },
                    err_fn,
                    None,
                )
                .map_err(|e| MicrophoneError::Audio(e.to_string()))?
        }
        cpal::SampleFormat::U16 => {
            device
                .build_input_stream(
                    &config,
                    move |data: &[u16], _: &cpal::InputCallbackInfo| {
                        // Downmix to mono
                        let mono_samples = downmix_to_mono_u16(data, channels);
                        // Resample to 16kHz
                        let resampled = resample_i16(&mono_samples, sample_rate);
                        let bytes = i16_samples_to_bytes_le(&resampled);
                        if let Err(e) = tx_product.send(bytes.clone()) {
                            eprintln!("[麦克风] 发送音频数据失败: {}", e);
                        }
                    },
                    err_fn,
                    None,
                )
                .map_err(|e| MicrophoneError::Audio(e.to_string()))?
        }
        cpal::SampleFormat::U8 => {
            device
                .build_input_stream(
                    &config,
                    move |data: &[u8], _: &cpal::InputCallbackInfo| {
                        // Downmix to mono
                        let mono_samples = downmix_to_mono_u8(data, channels);
                        // Resample to 16kHz
                        let resampled = resample_i16(&mono_samples, sample_rate);
                        let bytes = i16_samples_to_bytes_le(&resampled);
                        if let Err(e) = tx_product.send(bytes.clone()) {
                            eprintln!("[麦克风] 发送音频数据失败: {}", e);
                        }
                    },
                    err_fn,
                    None,
                )
                .map_err(|e| MicrophoneError::Audio(e.to_string()))?
        }
        _ => {
            return Err(MicrophoneError::Audio(format!(
                "不支持的音频格式: {:?}",
                stream_config.sample_format()
            )));
        }
    };

    Ok(stream)
}

/// 将多声道 i16 音频下混为单声道
fn downmix_to_mono_i16(samples: &[i16], channels: u16) -> Vec<i16> {
    if channels == 1 {
        return samples.to_vec();
    }
    let channels = channels as usize;
    let mut mono = Vec::with_capacity(samples.len() / channels);
    for chunk in samples.chunks_exact(channels) {
        let sum: i32 = chunk.iter().map(|&s| s as i32).sum();
        let avg = (sum / channels as i32) as i16;
        mono.push(avg);
    }
    mono
}

/// 将多声道 f32 音频下混为单声道
fn downmix_to_mono_f32(samples: &[f32], channels: u16) -> Vec<f32> {
    if channels == 1 {
        return samples.to_vec();
    }
    let channels = channels as usize;
    let mut mono = Vec::with_capacity(samples.len() / channels);
    for chunk in samples.chunks_exact(channels) {
        let sum: f32 = chunk.iter().sum();
        let avg = sum / channels as f32;
        mono.push(avg);
    }
    mono
}

/// 将多声道 u16 音频下混为单声道（并转换为 i16）
fn downmix_to_mono_u16(samples: &[u16], channels: u16) -> Vec<i16> {
    let channels = channels as usize;
    let mut mono = Vec::with_capacity(samples.len() / channels);
    for chunk in samples.chunks_exact(channels) {
        let sum: i32 = chunk.iter().map(|&s| s as i32 - 32768).sum();
        let avg = (sum / channels as i32) as i16;
        mono.push(avg);
    }
    mono
}

/// 将多声道 u8 音频下混为单声道（并转换为 i16）
fn downmix_to_mono_u8(samples: &[u8], channels: u16) -> Vec<i16> {
    let channels = channels as usize;
    let mut mono = Vec::with_capacity(samples.len() / channels);
    for chunk in samples.chunks_exact(channels) {
        let sum: i32 = chunk.iter().map(|&s| s as i32 - 128).sum();
        let avg = (sum / channels as i32) as i16;
        mono.push(avg);
    }
    mono
}

/// 简单线性重采样，从 source_rate 到 target_rate (16000)
fn resample_i16(samples: &[i16], source_rate: u32) -> Vec<i16> {
    let target_rate = 16000;
    if source_rate == target_rate {
        return samples.to_vec();
    }

    let ratio = target_rate as f64 / source_rate as f64;
    let num_output_samples = (samples.len() as f64 * ratio).ceil() as usize;
    let mut output = Vec::with_capacity(num_output_samples);

    for i in 0..num_output_samples {
        let src_pos = i as f64 / ratio;
        let src_idx = src_pos.floor() as usize;
        let frac = src_pos - src_idx as f64;

        if src_idx + 1 < samples.len() {
            let a = samples[src_idx] as f64;
            let b = samples[src_idx + 1] as f64;
            let val = a + frac * (b - a);
            output.push(val.round() as i16);
        } else if src_idx < samples.len() {
            output.push(samples[src_idx]);
        }
    }

    output
}

/// 将 i16 样本序列编码为 little-endian 的 s16le 字节。
pub fn i16_samples_to_bytes_le(samples: &[i16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

const EMPTY_BUFFER: Vec<u8> = Vec::new();

pub struct MicrophoneReader {
    on_close: Option<Box<dyn FnOnce() + Send>>,
    rx: mpsc::Receiver<Vec<u8>>,
    current_buffer: Vec<u8>,
    cur_index: usize,
    timeout: std::time::Duration,
}

impl MicrophoneReader {
    pub fn new(on_close: Box<dyn FnOnce() + Send>, rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            on_close: Some(on_close),
            rx,
            current_buffer: EMPTY_BUFFER,
            cur_index: 0,
            timeout: std::time::Duration::from_millis(20),
        }
    }

    pub fn set_timeout(&mut self, timeout: std::time::Duration) {
        self.timeout = timeout;
    }

    pub fn close(&mut self) {
        let on_close = self.on_close.take();
        on_close.map(|f| f());
        self.current_buffer = EMPTY_BUFFER;
        self.cur_index = 0;
    }
}

impl Read for MicrophoneReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let mut read_len = 0;
        for i in 0..buf_len {
            if self.cur_index >= self.current_buffer.len() {
                // 缓冲区空了，等待新数据
                if let Ok(new_buffer) = self.rx.recv_timeout(self.timeout) {
                    self.current_buffer = new_buffer;
                } else {
                    self.current_buffer = EMPTY_BUFFER;
                }
                self.cur_index = 0;
            }
            if self.cur_index >= self.current_buffer.len() {
                break;
            }
            buf[i] = self.current_buffer[self.cur_index];
            self.cur_index += 1;
            read_len += 1;
        }
        Ok(read_len)
    }
}

impl Drop for MicrophoneReader {
    fn drop(&mut self) {
        self.close();
    }
}
