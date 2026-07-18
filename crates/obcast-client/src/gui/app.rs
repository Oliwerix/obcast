//! The encoder GUI. Two halves:
//!  - `ObcastApp` (this file, `egui`/`eframe`, runs on the OS main thread):
//!    device/channel/gain controls, K-14 metering, Go Live control.
//!  - `controller` (a tokio task): owns the ffmpeg child process and the
//!    sse/uploader tasks, and forwards PCM from the audio engine into
//!    ffmpeg's stdin. The GUI only ever talks to it over an unbounded
//!    channel — never blocks a frame on network or process I/O.

use std::path::PathBuf;
use std::time::Duration;

use eframe::egui;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinHandle;

use obcast_proto::state::{PlayoutState, Rung, StreamProfile};

use crate::audio::{self, AudioHandle};
use crate::config::AppConfig;
use crate::gui::meter::{self, k14_meter, mini_meter, MeterReading};
use crate::shared::SharedState;
use crate::{encode, sse, uploader};
use std::sync::Arc;

pub fn run(cfg: AppConfig) -> eframe::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1040.0, 720.0])
            .with_min_inner_size([760.0, 520.0]),
        ..Default::default()
    };

    eframe::run_native(
        "OBCast Encoder",
        native_options,
        Box::new(move |_cc| Ok(Box::new(ObcastApp::new(rt, cfg)) as Box<dyn eframe::App>)),
    )
}

enum ControllerCmd {
    GoLive {
        profile: StreamProfile,
        base_url: String,
        stream: String,
        ingest_token: Option<String>,
        out_dir: PathBuf,
        sample_rate: u32,
    },
    StopLive,
}

struct ObcastApp {
    // Kept alive for the app's lifetime so spawned tasks keep running;
    // dropped (and its threads torn down) when the window closes.
    _rt: tokio::runtime::Runtime,

    audio: Arc<AudioHandle>,
    shared: Arc<SharedState>,
    cmd_tx: tokio_mpsc::UnboundedSender<ControllerCmd>,

    hosts: Vec<String>,
    selected_host: String,
    devices: Vec<audio::DeviceInfo>,
    selected_device: String,
    cfg: AppConfig,
    live: bool,
}

impl ObcastApp {
    fn new(rt: tokio::runtime::Runtime, cfg: AppConfig) -> Self {
        let (pcm_tx, pcm_rx) = tokio_mpsc::unbounded_channel();
        let audio = audio::spawn(pcm_tx);
        audio.set_mono(cfg.mono);
        audio.set_left_channel(cfg.left_channel);
        audio.set_right_channel(cfg.right_channel);
        audio.set_gain_db(cfg.gain_db);

        let shared = Arc::new(SharedState::new());
        let (cmd_tx, cmd_rx) = tokio_mpsc::unbounded_channel();
        rt.spawn(controller(cmd_rx, pcm_rx, audio.clone(), shared.clone()));

        let hosts = audio::list_hosts();
        let devices = audio::list_input_devices(&cfg.audio_host);
        if !cfg.device_name.is_empty() {
            audio.open(&cfg.audio_host, &cfg.device_name);
        }

        Self {
            _rt: rt,
            audio,
            shared,
            cmd_tx,
            hosts,
            selected_host: cfg.audio_host.clone(),
            devices,
            selected_device: cfg.device_name.clone(),
            cfg,
            live: false,
        }
    }

    fn persist_config(&self) {
        self.cfg.save();
    }

    fn profile(&self) -> StreamProfile {
        StreamProfile {
            segment_ms: self.cfg.segment_ms,
            rungs: vec![
                Rung {
                    id: 0,
                    name: "lo".into(),
                    bitrate_kbps: 32,
                },
                Rung {
                    id: 1,
                    name: "mid".into(),
                    bitrate_kbps: 128,
                },
                Rung {
                    id: 2,
                    name: "hd".into(),
                    bitrate_kbps: 320,
                },
            ],
        }
    }

    fn toggle_live(&mut self) {
        if self.live {
            let _ = self.cmd_tx.send(ControllerCmd::StopLive);
            self.live = false;
        } else {
            let sample_rate = self.audio.sample_rate().max(44_100);
            let ingest_token =
                (!self.cfg.ingest_token.is_empty()).then(|| self.cfg.ingest_token.clone());
            let _ = self.cmd_tx.send(ControllerCmd::GoLive {
                profile: self.profile(),
                base_url: self.cfg.server.clone(),
                stream: self.cfg.stream.clone(),
                ingest_token,
                out_dir: PathBuf::from(&self.cfg.out_dir),
                sample_rate,
            });
            self.live = true;
        }
        self.persist_config();
    }

    fn status_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("OBCast Encoder");
            ui.separator();
            ui.label(format!("stream: {}", self.cfg.stream));
            ui.separator();

            if let Ok(state) = self.shared.server.try_lock() {
                let playout = match state.playout.state {
                    PlayoutState::Playing => "🟢 playing",
                    PlayoutState::Paused => "🟡 paused",
                    PlayoutState::Stopped => "⚪ stopped",
                };
                ui.label(format!("server: {playout}"));
                ui.label(format!("lead {} ms", state.lead_ms));
                if let Some(seq) = state.live_seq {
                    ui.label(format!("live seq {seq}"));
                }
            } else {
                ui.label("server: (no link yet)");
            }
            ui.separator();
            if let Some(seq) = self.shared.last_uploaded_seq() {
                ui.label(format!(
                    "uploaded seq {seq} @ {} kbps",
                    self.shared.throughput_kbps()
                ));
            }

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let (text, color) = if self.live {
                    ("■ Stop", egui::Color32::from_rgb(0x8a, 0x1f, 0x1f))
                } else {
                    ("● Go Live", egui::Color32::from_rgb(0x1f, 0x6f, 0x2a))
                };
                let btn = egui::Button::new(
                    egui::RichText::new(text)
                        .strong()
                        .color(egui::Color32::WHITE),
                )
                .fill(color);
                if ui
                    .add_enabled(self.audio.is_running(), btn)
                    .on_disabled_hover_text("Open an input device first")
                    .clicked()
                {
                    self.toggle_live();
                }
            });
        });
    }

    fn device_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Audio Subsystem");
        ui.horizontal(|ui| {
            egui::ComboBox::from_id_salt("host_combo")
                .width(260.0)
                .selected_text(if self.selected_host.is_empty() {
                    "(platform default)".to_string()
                } else {
                    self.selected_host.clone()
                })
                .show_ui(ui, |ui| {
                    if ui
                        .selectable_value(
                            &mut self.selected_host,
                            String::new(),
                            "(platform default)",
                        )
                        .changed()
                    {
                        self.devices = audio::list_input_devices(&self.selected_host);
                        self.selected_device.clear();
                    }
                    for h in self.hosts.clone() {
                        if ui
                            .selectable_value(&mut self.selected_host, h.clone(), h)
                            .changed()
                        {
                            self.devices = audio::list_input_devices(&self.selected_host);
                            self.selected_device.clear();
                        }
                    }
                });
            if self.selected_host != self.cfg.audio_host {
                self.cfg.audio_host = self.selected_host.clone();
                self.persist_config();
            }
        });

        ui.separator();
        ui.heading("Input Device");
        ui.horizontal(|ui| {
            egui::ComboBox::from_id_salt("device_combo")
                .width(260.0)
                .selected_text(if self.selected_device.is_empty() {
                    "(choose a device)".to_string()
                } else {
                    self.selected_device.clone()
                })
                .show_ui(ui, |ui| {
                    for d in &self.devices {
                        let label =
                            format!("{} — {} ch @ {} Hz", d.name, d.channels, d.sample_rate);
                        ui.selectable_value(&mut self.selected_device, d.name.clone(), label);
                    }
                });
            if ui
                .button("⟳")
                .on_hover_text("Refresh device list")
                .clicked()
            {
                self.devices = audio::list_input_devices(&self.selected_host);
            }
        });
        ui.horizontal(|ui| {
            if ui.button("Open").clicked() && !self.selected_device.is_empty() {
                self.audio.open(&self.selected_host, &self.selected_device);
                self.cfg.device_name = self.selected_device.clone();
                self.persist_config();
            }
            if ui.button("Close").clicked() {
                self.audio.close();
            }
        });
        if self.audio.is_running() {
            ui.colored_label(
                egui::Color32::from_rgb(0x35, 0xc7, 0x5f),
                format!(
                    "open: {} ({} ch @ {} Hz)",
                    self.audio.device_name(),
                    self.audio.device_channels(),
                    self.audio.sample_rate()
                ),
            );
        } else if let Some(err) = self.audio.last_error() {
            ui.colored_label(
                egui::Color32::from_rgb(0xe2, 0x3d, 0x3d),
                format!("error: {err}"),
            );
        } else {
            ui.label("no device open");
        }

        ui.separator();
        ui.heading("Channel Map");
        ui.label("Pick which of this device's channels feed L/R — handy for a multichannel snake where the mic isn't on channel 1/2.");
        let channels = self.audio.device_channels();

        let mut mono = self.audio.mono();
        if ui
            .checkbox(&mut mono, "Mono (duplicate one source channel to L+R)")
            .changed()
        {
            self.audio.set_mono(mono);
            self.cfg.mono = mono;
            self.persist_config();
        }

        let mut left = self.audio.left_channel();
        let mut right = self.audio.right_channel();
        ui.horizontal(|ui| {
            ui.label(if mono {
                "Source channel:"
            } else {
                "Left channel:"
            });
            egui::ComboBox::from_id_salt("left_ch")
                .selected_text(channel_label(left, channels))
                .show_ui(ui, |ui| {
                    for ch in 0..channels.max(1) {
                        ui.selectable_value(&mut left, ch, channel_label(ch, channels));
                    }
                });
        });
        if !mono {
            ui.horizontal(|ui| {
                ui.label("Right channel: ");
                egui::ComboBox::from_id_salt("right_ch")
                    .selected_text(channel_label(right, channels))
                    .show_ui(ui, |ui| {
                        for ch in 0..channels.max(1) {
                            ui.selectable_value(&mut right, ch, channel_label(ch, channels));
                        }
                    });
            });
        }
        if left != self.audio.left_channel() {
            self.audio.set_left_channel(left);
            self.cfg.left_channel = left;
            self.persist_config();
        }
        if right != self.audio.right_channel() {
            self.audio.set_right_channel(right);
            self.cfg.right_channel = right;
            self.persist_config();
        }

        ui.separator();
        ui.heading("Gain");
        let mut gain = self.audio.gain_db();
        let resp = ui.add(
            egui::Slider::new(&mut gain, -24.0..=24.0)
                .suffix(" dB")
                .text("input gain"),
        );
        if resp.changed() {
            self.audio.set_gain_db(gain);
            self.cfg.gain_db = gain;
        }
        if resp.drag_stopped() {
            self.persist_config();
        }
        if ui.button("Reset gain to 0 dB").clicked() {
            self.audio.set_gain_db(0.0);
            self.cfg.gain_db = 0.0;
            self.persist_config();
        }

        ui.separator();
        ui.heading("All Input Channels");
        ui.label(
            "Every channel this device offers — find which one has signal, then assign it above.",
        );
        egui::ScrollArea::vertical()
            .max_height(240.0)
            .show(ui, |ui| {
                let peaks = self.audio.channel_peaks();
                for (i, peak) in peaks.iter().enumerate() {
                    let ch = i as u16;
                    ui.horizontal(|ui| {
                        ui.label(format!("{:>3}", ch + 1));
                        mini_meter(ui, *peak, egui::vec2(150.0, 12.0));
                        ui.label(format!("{:>5.1} dB", meter::linear_to_dbfs(*peak)));
                        if ui.small_button("→ L").clicked() {
                            self.audio.set_left_channel(ch);
                            self.cfg.left_channel = ch;
                            self.persist_config();
                        }
                        if !self.audio.mono() && ui.small_button("→ R").clicked() {
                            self.audio.set_right_channel(ch);
                            self.cfg.right_channel = ch;
                            self.persist_config();
                        }
                    });
                }
                if peaks.is_empty() {
                    ui.label("(open a device to see its channels)");
                }
            });

        ui.separator();
        ui.heading("Stream Target");
        ui.add_enabled_ui(!self.live, |ui| {
            egui::Grid::new("target_grid")
                .num_columns(2)
                .show(ui, |ui| {
                    ui.label("Server URL:");
                    ui.text_edit_singleline(&mut self.cfg.server);
                    ui.end_row();

                    ui.label("Stream:");
                    ui.text_edit_singleline(&mut self.cfg.stream);
                    ui.end_row();

                    ui.label("Ingest token:");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.ingest_token).password(true));
                    ui.end_row();

                    ui.label("Segment (ms):");
                    ui.add(egui::DragValue::new(&mut self.cfg.segment_ms).range(500..=10_000));
                    ui.end_row();

                    ui.label("Buffer dir:");
                    ui.text_edit_singleline(&mut self.cfg.out_dir);
                    ui.end_row();
                });
        });
        if !self.live {
            ui.small("(target settings lock while live; stop to change them)");
        }
    }

    fn meter_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Levels — K-14");
        let ((vu_l, ppm_l), (vu_r, ppm_r)) = self.audio.meters();
        let clipped_l = self.audio.take_clip_l();
        let clipped_r = self.audio.take_clip_r();
        let mono = self.audio.mono();

        ui.horizontal(|ui| {
            let reading_l = MeterReading {
                vu_db: vu_l,
                ppm_db: ppm_l,
                clipped: clipped_l,
            };
            let label_l = if mono { "MONO" } else { "L" };
            let resp_l = k14_meter(ui, label_l, &reading_l, egui::vec2(90.0, 280.0));
            if resp_l.clicked() {
                self.audio.reset_clips();
            }

            if !mono {
                let reading_r = MeterReading {
                    vu_db: vu_r,
                    ppm_db: ppm_r,
                    clipped: clipped_r,
                };
                let resp_r = k14_meter(ui, "R", &reading_r, egui::vec2(90.0, 280.0));
                if resp_r.clicked() {
                    self.audio.reset_clips();
                }
            }

            ui.add_space(16.0);
            ui.vertical(|ui| {
                ui.label(
                    egui::RichText::new("K-14: 0 = -14 dBFS, 14 dB headroom to clip.").small(),
                );
                ui.label(format!("L  vu {vu_l:>5.1} dB   ppm {ppm_l:>5.1} dB"));
                if !mono {
                    ui.label(format!("R  vu {vu_r:>5.1} dB   ppm {ppm_r:>5.1} dB"));
                }
                if clipped_l || clipped_r {
                    ui.colored_label(
                        egui::Color32::from_rgb(0xff, 0x40, 0x40),
                        "CLIP — click a meter to clear",
                    );
                }
            });
        });
    }
}

impl eframe::App for ObcastApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Meters need to visibly move; repaint at ~30fps regardless of input.
        ui.ctx().request_repaint_after(Duration::from_millis(33));

        egui::Panel::top("top").show(ui, |ui| {
            ui.add_space(4.0);
            self.status_bar(ui);
            ui.add_space(4.0);
        });

        egui::Panel::left("controls")
            .min_size(360.0)
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    self.device_panel(ui);
                });
            });

        egui::CentralPanel::default().show(ui, |ui| {
            self.meter_panel(ui);
        });
    }
}

fn channel_label(ch: u16, total: u16) -> String {
    match (ch, total) {
        (0, 2) => "1 (L)".to_string(),
        (1, 2) => "2 (R)".to_string(),
        _ => format!("{}", ch + 1),
    }
}

/// Owns the ffmpeg child, the sse/uploader tasks, and the PCM feed into
/// ffmpeg's stdin. Multiplexes GUI commands and PCM blocks in one loop so
/// there's a single owner for the stdin handle — no locking needed.
async fn controller(
    mut cmd_rx: tokio_mpsc::UnboundedReceiver<ControllerCmd>,
    mut pcm_rx: tokio_mpsc::UnboundedReceiver<Vec<f32>>,
    audio: Arc<AudioHandle>,
    shared: Arc<SharedState>,
) {
    let mut stdin: Option<ChildStdin> = None;
    let mut child: Option<Child> = None;
    let mut sse_handle: Option<JoinHandle<()>> = None;
    let mut upload_handle: Option<JoinHandle<()>> = None;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ControllerCmd::GoLive { profile, base_url, stream, ingest_token, out_dir, sample_rate }) => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;

                        if let Err(err) = tokio::fs::create_dir_all(&out_dir).await {
                            tracing::error!(error = %err, "failed to create buffer dir");
                            continue;
                        }

                        match encode::spawn(&encode::Source::Pcm { sample_rate }, &profile, &out_dir) {
                            Ok(mut c) => {
                                stdin = c.stdin.take();
                                child = Some(c);
                                audio.set_live(true);

                                let client = reqwest::Client::new();
                                sse_handle = Some(tokio::spawn(sse::run(
                                    client.clone(),
                                    base_url.clone(),
                                    stream.clone(),
                                    shared.clone(),
                                )));
                                upload_handle = Some(tokio::spawn(uploader::run(
                                    client,
                                    uploader::Config { base_url, stream, ingest_token, out_dir, profile },
                                    shared.clone(),
                                )));
                                tracing::info!("live: encoder pipeline started");
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "failed to spawn ffmpeg");
                            }
                        }
                    }
                    Some(ControllerCmd::StopLive) => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
                        tracing::info!("live: encoder pipeline stopped");
                    }
                    None => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
                        return;
                    }
                }
            }
            pcm = pcm_rx.recv() => {
                let Some(block) = pcm else { return };
                if let Some(s) = stdin.as_mut() {
                    let mut bytes = Vec::with_capacity(block.len() * 4);
                    for sample in &block {
                        bytes.extend_from_slice(&sample.to_le_bytes());
                    }
                    if let Err(err) = s.write_all(&bytes).await {
                        tracing::warn!(error = %err, "pcm write to ffmpeg failed, dropping feed");
                        stdin = None;
                    }
                }
            }
        }
    }
}

async fn stop_live(
    stdin: &mut Option<ChildStdin>,
    child: &mut Option<Child>,
    sse_handle: &mut Option<JoinHandle<()>>,
    upload_handle: &mut Option<JoinHandle<()>>,
    audio: &Arc<AudioHandle>,
) {
    audio.set_live(false);
    // Dropping stdin sends EOF, letting ffmpeg flush and finalize the
    // segment it's mid-writing instead of leaving a truncated file behind.
    *stdin = None;
    if let Some(h) = sse_handle.take() {
        h.abort();
    }
    if let Some(h) = upload_handle.take() {
        h.abort();
    }
    if let Some(mut c) = child.take() {
        let _ = tokio::time::timeout(Duration::from_secs(3), c.wait()).await;
        let _ = c.start_kill();
    }
}
