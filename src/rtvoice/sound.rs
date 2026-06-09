use rodio::{OutputStream, OutputStreamHandle, PlayError, Sink, Source};
use std::{
    fmt::{Display, Formatter},
    sync::{
        Arc, Mutex, atomic::{AtomicBool, Ordering}, mpsc
    },
    thread,
};

pub struct SoundConfig {}

#[derive(Debug)]
pub enum SoundError {
    Audio(String),
    Play(PlayError),
    Stoped,
}

enum SoundCommand {
    Play {
        audio: Vec<u8>,
        pcm_format: PcmFormat,
    },
    Stopping,
}

pub struct Sound {
    running: Arc<AtomicBool>,
    worker_thread: Option<thread::JoinHandle<()>>,
    cmd_sender: mpsc::Sender<SoundCommand>,
}

impl Sound {
    pub fn start(stream_handle: OutputStreamHandle) -> Result<Self, SoundError> {

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let (tx, rx) = mpsc::channel::<SoundCommand>();

        let sink = Sink::try_new(&stream_handle).map_err(|e| SoundError::Play(e))?;
        let worker = thread::spawn(move || Self::run_work(running_clone, sink, rx));

        let sound = Self {
            running,
            worker_thread: Some(worker),
            cmd_sender: tx.clone(),
        };

        Ok(sound)
    }

    fn run_work(
        r: Arc<AtomicBool>,
        sink: Sink,
        rx: mpsc::Receiver<SoundCommand>
    ) {
        println!("Sound Worker thread started");
        while r.load(Ordering::Relaxed) {
            let cmd = rx.recv();
            match cmd {
                Ok(cmd) => match cmd {
                    SoundCommand::Play { audio, pcm_format } => {
                        play_audio(&sink, audio, pcm_format);
                    }
                    SoundCommand::Stopping => {
                        break;
                    }
                },
                Err(e) => {
                    eprintln!("Sound Error receiving command: {:?}", e);
                    continue;
                }
            }
        }
        r.store(false, Ordering::Relaxed);
        println!("Sound Worker thread stopped");
        sink.detach();
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn check_running(&self) -> Result<(), SoundError> {
        if !self.is_running() {
            Err(SoundError::Stoped)
        } else {
            Ok(())
        }
    }

    pub fn play(&self, audio: Vec<u8>, pcm_format: PcmFormat) -> Result<(), SoundError> {
        self.check_running()?;
        println!("Sound Play command sent: audio len: {}", audio.len());
        self.cmd_sender
            .send(SoundCommand::Play { audio, pcm_format })
            .map_err(|_| SoundError::Stoped)?;
        Ok(())
    }

    pub fn stop(&mut self) {
        if !self.is_running() {
            return;
        }
        self.running.store(false, Ordering::Relaxed);
        let _ = self.cmd_sender.send(SoundCommand::Stopping);
        let join_handle = self.worker_thread.take().unwrap();
        let _ = join_handle.join();
    }
}

impl Drop for Sound {
    fn drop(&mut self) {
        self.stop();
    }
}

/// PCM 数据格式（与 Java `Config.DEFAULT_PCM` / `Config.PCM_S16LE` 含义一致）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PcmFormat {
    PcmF32le,
    PcmS16le,
}

impl PcmFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PcmF32le => "pcm",
            Self::PcmS16le => "pcm_s16le",
        }
    }
}

impl From<&str> for PcmFormat {
    fn from(s: &str) -> Self {
        match s {
            "pcm" => Self::PcmF32le,
            "pcm_s16le" => Self::PcmS16le,
            _ => panic!("Invalid PCM format: {}", s),
        }
    }
}

// A simple source for streaming raw PCM data
struct PcmSource {
    data: Vec<u8>,
    pos: usize,
    sample_format: PcmFormat,
}

impl PcmSource {
    fn new(data: Vec<u8>, sample_format: PcmFormat) -> Self {
        Self {
            data,
            pos: 0,
            sample_format,
        }
    }
}

impl Display for PcmSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "PcmSource {{ data: {}, pos: {}, sample_format: {} }}",
            self.data.len(),
            self.pos,
            self.sample_format.as_str()
        )
    }
}

impl Iterator for PcmSource {
    type Item = i16; // Rodio uses i16 for most cases

    fn next(&mut self) -> Option<Self::Item> {
        match self.sample_format {
            PcmFormat::PcmS16le => {
                if self.pos + 2 > self.data.len() {
                    return None;
                }
                let bytes = [self.data[self.pos], self.data[self.pos + 1]];
                self.pos += 2;
                Some(i16::from_le_bytes(bytes))
            }
            PcmFormat::PcmF32le => {
                if self.pos + 4 > self.data.len() {
                    return None;
                }
                let bytes = [
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ];
                self.pos += 4;
                let f = f32::from_le_bytes(bytes);
                // Convert f32 to i16 (scale to i16's range)
                Some((f * i16::MAX as f32) as i16)
            }
        }
    }
}

impl Source for PcmSource {
    fn current_frame_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        1
    }

    fn sample_rate(&self) -> u32 {
        24000
    }

    fn total_duration(&self) -> Option<std::time::Duration> {
        None
    }
}

fn play_audio(sink: &Sink, audio: Vec<u8>, pcm_format: PcmFormat) {
    let source = PcmSource::new(audio, pcm_format);
    println!("Sound Play command received: {}", source);
    play_pcm_source(sink, source);
}

fn play_pcm_source(sink: &Sink, source: PcmSource) {
    sink.append(source);
}
