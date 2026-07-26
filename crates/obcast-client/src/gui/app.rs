//! The encoder GUI. Two halves:
//!  - `ObcastApp` (this file, `egui`/`eframe`, runs on the OS main thread):
//!    device/channel/gain controls, VU/PPM metering, Go Live control.
//!  - `controller` (a tokio task): owns the ffmpeg child process and the
//!    sse/uploader tasks, and forwards PCM from the audio engine into
//!    ffmpeg's stdin. The GUI only ever talks to it over an unbounded
//!    channel — never blocks a frame on network or process I/O.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use eframe::egui;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinHandle;

use std::collections::BTreeSet;

use obcast_proto::control::{LogEntry, LogLevel};
use obcast_proto::state::{PlayoutState, RungId, StreamProfile};

use crate::audio::{self, AudioHandle};
use crate::config::{AppConfig, PeakMode};
use crate::gui::meter::{self, level_meter, mini_meter, MeterReading};
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
        auto_start_buffer_ms: Option<u32>,
        bootstrap_rung: RungId,
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
    /// A rung checkbox click while live, staged behind a confirmation modal
    /// (see `rungs_panel`) rather than applied immediately — toggling a rung
    /// mid-session restarts the encoder pipeline (brief audio gap), so the
    /// operator has to confirm it. `None` while not live or no click pending.
    /// The bool is the click's target state (true = enabling, false =
    /// disabling) so the modal can describe what's about to happen.
    pending_rung_toggle: Option<(RungId, bool)>,
    /// Whether the operator log panel (bottom of the window) is open.
    show_log: bool,
    /// `SharedState::log_seq()` value at the moment the operator last
    /// dismissed the status bar's infobar summary of the latest log line.
    /// The infobar stays hidden until a genuinely new line pushes the
    /// counter past this — see `status_bar`.
    dismissed_log_seq: u64,
    /// "All Input Channels" bank source data — resampled once/sec rather
    /// than every repaint (see `sample_channel_peaks`). `channel_peaks_display`
    /// eases toward `channel_peaks_target` every frame so the bars still
    /// read as smoothly moving despite the coarser sample rate.
    channel_peaks_target: Vec<f32>,
    channel_peaks_display: Vec<f32>,
    channel_peaks_sampled_at: Option<Instant>,

    /// Last-`HISTORY_LEN` samples (oldest first) for the Link panel's
    /// rolling graphs, sampled at `HISTORY_SAMPLE_INTERVAL` regardless of
    /// the ~30fps repaint rate — see `link_panel`.
    buffer_history: VecDeque<f32>,
    bandwidth_history: VecDeque<f32>,
    /// Rung id as a plain float; only sampled while something is actually
    /// playing (skipped, not zero-filled, while stopped/unknown).
    quality_history: VecDeque<f32>,
    /// % of the outstanding buffer (`ServerState::coverage`) held at the top
    /// rung; only sampled when the server has reported any coverage at all
    /// (skipped, not zero-filled, otherwise) — see `link_panel`.
    buffer_quality_history: VecDeque<f32>,
    history_sampled_at: Option<Instant>,

    /// When the app started — an arbitrary but stable zero point for the
    /// connection-fail flash's continuous blink phase (see `flash_color`).
    created_at: Instant,
    /// Whether either selected input was clipping as of the last frame —
    /// used to detect the rising edge of a fresh clip, since the underlying
    /// `AudioHandle::clip_l`/`clip_r` flags are a latch that stays set until
    /// the operator clears it by clicking a level meter (`meter_panel`), not
    /// a one-shot event. Without edge detection an unreset clip would keep
    /// restarting the flash sequence every frame.
    was_clipping: bool,
    /// `Some(t)` while a clip-flash sequence is running (started at `t`);
    /// cleared once `CLIP_FLASH_COUNT` on/off cycles have elapsed. See
    /// `flash_color`.
    clip_flash_started_at: Option<Instant>,
    /// Whether the server link was down as of the last frame — edge-detected
    /// the same way as `was_clipping`, so the link-outage alarm logs once
    /// per outage rather than every frame it stays down. See `flash_color`.
    was_link_down: bool,
    /// Whether the current outage has already been logged as critical (past
    /// `FLASH_BUFFER_THRESHOLD_MS`) — reset once the link recovers, so a
    /// later outage can re-escalate and log again. See `flash_color`.
    link_down_logged_critical: bool,
}

/// How many full on/off blinks a fresh input clip flashes the window
/// border.
const CLIP_FLASH_COUNT: u32 = 4;
/// Duration of each on or off half-cycle of a border flash (clip or
/// connection-fail).
const FLASH_HALF_PERIOD: Duration = Duration::from_millis(200);
/// `link_panel`'s "Buffer" estimate, below which a connection failure
/// flashes red instead of yellow — the point past which dropout is close
/// enough to demand more urgency than a plain heads-up.
const FLASH_BUFFER_THRESHOLD_MS: u32 = 45_000;

/// How far back the Link panel's rolling graphs look.
const HISTORY_WINDOW: Duration = Duration::from_secs(60);
/// How often a new point is sampled — independent of the GUI's ~30fps
/// repaint, so the graphs don't fill up with 1800 near-identical points.
const HISTORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const HISTORY_LEN: usize = (HISTORY_WINDOW.as_secs() / HISTORY_SAMPLE_INTERVAL.as_secs()) as usize;

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
            pending_rung_toggle: None,
            show_log: false,
            dismissed_log_seq: 0,
            channel_peaks_target: Vec::new(),
            channel_peaks_display: Vec::new(),
            channel_peaks_sampled_at: None,
            buffer_history: VecDeque::new(),
            bandwidth_history: VecDeque::new(),
            quality_history: VecDeque::new(),
            buffer_quality_history: VecDeque::new(),
            history_sampled_at: None,
            created_at: Instant::now(),
            was_clipping: false,
            clip_flash_started_at: None,
            was_link_down: false,
            link_down_logged_critical: false,
        }
    }

    fn persist_config(&self) {
        self.cfg.save();
    }

    /// Refresh `channel_peaks_target` from the audio thread at most once a
    /// second, then ease `channel_peaks_display` toward it every frame. The
    /// whole window still repaints at ~30fps regardless (see `ui()`), so
    /// this doesn't reduce paint calls — it throttles the actual per-frame
    /// cost, which was `channel_peaks()`'s lock-and-clone plus a full
    /// per-channel re-layout, repeated 30x/sec for a view nobody reads at
    /// that rate.
    fn sample_channel_peaks(&mut self) {
        let due = match self.channel_peaks_sampled_at {
            None => true,
            Some(at) => at.elapsed() >= Duration::from_secs(1),
        };
        if due {
            self.channel_peaks_target = self.audio.channel_peaks();
            self.channel_peaks_sampled_at = Some(Instant::now());
        }
        if self.channel_peaks_display.len() != self.channel_peaks_target.len() {
            self.channel_peaks_display
                .resize(self.channel_peaks_target.len(), 0.0);
        }
        const EASE: f32 = 0.15;
        for (disp, target) in self
            .channel_peaks_display
            .iter_mut()
            .zip(self.channel_peaks_target.iter())
        {
            *disp += (*target - *disp) * EASE;
        }
    }

    /// The ladder this session actually encodes/uploads — the full default
    /// ladder narrowed to whatever the operator has enabled (see
    /// `StreamProfile::filtered`; never empty).
    fn profile(&self) -> StreamProfile {
        let enabled: BTreeSet<RungId> = self.cfg.enabled_rungs.iter().copied().collect();
        StreamProfile::default_ladder(self.cfg.segment_ms).filtered(&enabled)
    }

    fn go_live_cmd(&self) -> ControllerCmd {
        let sample_rate = self.audio.sample_rate().max(44_100);
        let ingest_token =
            (!self.cfg.ingest_token.is_empty()).then(|| self.cfg.ingest_token.clone());
        let auto_start_buffer_ms = self
            .cfg
            .auto_start
            .then_some(self.cfg.auto_start_buffer_secs * 1000);
        ControllerCmd::GoLive {
            profile: self.profile(),
            base_url: self.cfg.server.clone(),
            stream: self.cfg.stream.clone(),
            ingest_token,
            out_dir: PathBuf::from(&self.cfg.out_dir),
            sample_rate,
            auto_start_buffer_ms,
            bootstrap_rung: self.cfg.default_rung,
        }
    }

    fn toggle_live(&mut self) {
        if self.live {
            let _ = self.cmd_tx.send(ControllerCmd::StopLive);
            self.live = false;
        } else {
            let cmd = self.go_live_cmd();
            let _ = self.cmd_tx.send(cmd);
            self.live = true;
        }
        self.persist_config();
    }

    /// Left side (heading + link/upload status, unbounded in length — a long
    /// operator log message or server `detail` string used to push the Log
    /// and Stop/Go Live controls on the right straight off the window edge,
    /// since a plain `ui.horizontal` never wraps or clips) is rendered via
    /// `egui::Sides` with `shrink_left().truncate()`: the right side (fixed
    /// controls) is laid out first at its natural size, and the left side
    /// only gets whatever width remains, eliding its text with "…" instead
    /// of growing past it. Status strings are built into local `String`s
    /// before the `Sides::show` call so the read-only left closure doesn't
    /// need to borrow `self` at all, leaving the right closure free to hold
    /// the `&mut self` it needs for the buttons.
    fn status_bar(&mut self, ui: &mut egui::Ui) {
        let stream_label = format!("stream: {}", self.cfg.stream);

        let server_status = if let Ok(state) = self.shared.server.try_lock() {
            let playout = match state.playout.state {
                PlayoutState::Playing => "🟢 playing",
                PlayoutState::Paused => "🟡 paused",
                PlayoutState::Stopped => "⚪ stopped",
                PlayoutState::Stalled => "🟠 stalled",
                PlayoutState::Error => "🔴 error",
            };
            let mut s = format!("server: {playout}");
            if let Some(detail) = &state.playout.detail {
                s.push_str(&format!(" ({detail})"));
            }
            s.push_str(&format!("   lead {} ms", state.lead_ms));
            if let Some(seq) = state.live_seq {
                s.push_str(&format!("   live seq {seq}"));
            }
            s
        } else {
            "server: (no link yet)".to_string()
        };

        let uploaded_label = self.shared.last_uploaded_seq().map(|seq| {
            format!(
                "uploaded seq {seq} @ {} kbps",
                self.shared.throughput_kbps()
            )
        });

        let log_seq = self.shared.log_seq();
        // Hidden once dismissed (see `dismissed_log_seq`'s doc) until a
        // genuinely new line is pushed and bumps the counter past it.
        let log_entry = (log_seq > self.dismissed_log_seq)
            .then(|| self.shared.latest_log())
            .flatten()
            .map(|entry| {
                (
                    log_level_color(entry.level),
                    format!("{} {}", log_level_tag(entry.level), entry.message),
                )
            });

        let (dismiss_clicked, _) = egui::Sides::new().shrink_left().truncate().show(
            ui,
            |ui| {
                ui.heading("OBCast Encoder");
                ui.separator();
                ui.label(stream_label);
                ui.separator();
                ui.label(server_status);
                if let Some(uploaded_label) = uploaded_label {
                    ui.separator();
                    ui.label(uploaded_label);
                }
                let mut dismiss = false;
                if let Some((color, text)) = log_entry {
                    ui.separator();
                    let resp = ui
                        .add(
                            egui::Label::new(egui::RichText::new(text).color(color))
                                .sense(egui::Sense::click()),
                        )
                        .on_hover_text("Click to dismiss");
                    if resp.clicked() {
                        dismiss = true;
                    }
                }
                dismiss
            },
            |ui| {
                let log_text = if self.show_log { "Log ▼" } else { "Log ▲" };
                if ui
                    .button(log_text)
                    .on_hover_text("Show the operator status/error log")
                    .clicked()
                {
                    self.show_log = !self.show_log;
                }
                ui.add_space(8.0);
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
            },
        );
        if dismiss_clicked {
            self.dismissed_log_seq = log_seq;
        }
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

        // Stream Target now sits where "Channel Map" used to (see
        // `channel_map_panel` below for why that section was folded away).
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

                    ui.label("Auto-start:");
                    ui.horizontal(|ui| {
                        ui.checkbox(&mut self.cfg.auto_start, "after buffer of")
                            .on_hover_text(
                                "Server starts playout on its own once this much buffer has \
                                 accumulated, instead of waiting for a web operator. A manual \
                                 start always takes precedence.",
                            );
                        ui.add_enabled(
                            self.cfg.auto_start,
                            egui::DragValue::new(&mut self.cfg.auto_start_buffer_secs)
                                .range(10..=3600)
                                .suffix(" s"),
                        )
                        .on_hover_text(
                            "Must stay comfortably under the server's DVR window (5 min by \
                             default) — the server can never buffer more than that while \
                             stopped, so a larger request will never be satisfied.",
                        );
                    });
                    ui.end_row();
                });
        });
        if !self.live {
            ui.small("(target settings lock while live; stop to change them)");
        }

        ui.separator();
        ui.heading("Gain");
        let mut gain = self.audio.gain_db();
        let resp = ui.add(
            egui::Slider::new(&mut gain, -10.0..=24.0)
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
        self.channel_map_panel(ui);

        // Unlike the rest of "Stream Target" above, rung selection stays
        // interactive while live — toggling one restarts the pipeline (see
        // `apply_rung_toggle`), but that's confirmed via a modal rather than
        // requiring a full manual Stop first.
        ui.separator();
        ui.heading("Rungs");
        self.rungs_panel(ui);
    }

    /// Mono toggle + every channel this device offers, each with L/R
    /// assignment buttons that also show the current assignment (a
    /// `selectable_label`, highlighted when that channel is the one already
    /// feeding L or R). Replaces the old separate "Channel Map" section,
    /// whose L/R dropdowns were pure duplication of what these per-channel
    /// buttons already do — mono (the one bit of that section's behavior
    /// these buttons can't express on their own) moved here instead.
    /// Collapsible (collapsed by default) so a routine session can shrink it
    /// down to just the assigned channel numbers, shown right in the header,
    /// once L/R are dialed in.
    fn channel_map_panel(&mut self, ui: &mut egui::Ui) {
        let mono = self.audio.mono();
        let left = self.audio.left_channel();
        let right = self.audio.right_channel();
        let header = if mono {
            format!("All Input Channels — source: {}", left + 1)
        } else {
            format!("All Input Channels — L: {}  R: {}", left + 1, right + 1)
        };

        egui::CollapsingHeader::new(header)
            .id_salt("all_input_channels")
            .default_open(false)
            .show(ui, |ui| {
                ui.label(
                    "Every channel this device offers — find which one has signal, then \
                     click L/R to assign it.",
                );

                let mut mono = self.audio.mono();
                if ui
                    .checkbox(&mut mono, "Mono (duplicate one source channel to L+R)")
                    .changed()
                {
                    self.audio.set_mono(mono);
                    self.cfg.mono = mono;
                    self.persist_config();
                }

                self.sample_channel_peaks();
                let left = self.audio.left_channel();
                let right = self.audio.right_channel();
                egui::ScrollArea::vertical()
                    .max_height(240.0)
                    .show(ui, |ui| {
                        let peaks = self.channel_peaks_display.clone();
                        for (i, peak) in peaks.iter().enumerate() {
                            let ch = i as u16;
                            ui.horizontal(|ui| {
                                ui.label(format!("{:>3}", ch + 1));
                                mini_meter(ui, *peak, egui::vec2(150.0, 12.0));
                                ui.label(format!("{:>5.1} dB", meter::linear_to_dbfs(*peak)));
                                let left_label = if mono { "Source" } else { "L" };
                                if ui.selectable_label(left == ch, left_label).clicked() {
                                    self.audio.set_left_channel(ch);
                                    self.cfg.left_channel = ch;
                                    self.persist_config();
                                }
                                if !mono && ui.selectable_label(right == ch, "R").clicked() {
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
            });
    }

    /// Per-rung enable/disable checkboxes plus the "Default quality"
    /// (bootstrap rung) picker. Any rung may be disabled, including the
    /// lowest — the scheduler always treats whatever's left as the
    /// survival rung (see `StreamProfile::filtered`) — except the last
    /// remaining enabled one, which the UI refuses to let go empty.
    fn rungs_panel(&mut self, ui: &mut egui::Ui) {
        let ladder = StreamProfile::default_ladder(self.cfg.segment_ms);
        let enabled_count = self.cfg.enabled_rungs.len();

        for rung in &ladder.rungs {
            let mut checked = self.cfg.enabled_rungs.contains(&rung.id);
            let is_only_one_left = checked && enabled_count <= 1;
            let codec_tag = match rung.codec {
                obcast_proto::state::AacCodec::He => " · HE-AAC",
                obcast_proto::state::AacCodec::Lc => "",
            };
            ui.horizontal(|ui| {
                ui.add_enabled_ui(!is_only_one_left, |ui| {
                    let resp = ui.checkbox(
                        &mut checked,
                        format!("{} — {} kbps{codec_tag}", rung.name, rung.bitrate_kbps),
                    );
                    let resp = if is_only_one_left {
                        resp.on_hover_text("at least one rung must stay enabled")
                    } else if self.live {
                        resp.on_hover_text("restarts the encoder pipeline (confirmation required)")
                    } else {
                        resp
                    };
                    if resp.changed() {
                        if self.live {
                            self.pending_rung_toggle = Some((rung.id, checked));
                        } else {
                            self.apply_rung_toggle(rung.id, checked);
                        }
                    }
                });
            });
        }

        let enabled_rungs: Vec<_> = ladder
            .rungs
            .iter()
            .filter(|r| self.cfg.enabled_rungs.contains(&r.id))
            .collect();
        let selected_label = enabled_rungs
            .iter()
            .find(|r| r.id == self.cfg.default_rung)
            .or(enabled_rungs.first())
            .map(|r| format!("{} ({} kbps)", r.name, r.bitrate_kbps))
            .unwrap_or_else(|| "—".to_string());
        let before = self.cfg.default_rung;
        egui::ComboBox::from_label("Default quality")
            .selected_text(selected_label)
            .show_ui(ui, |ui| {
                for rung in &enabled_rungs {
                    ui.selectable_value(
                        &mut self.cfg.default_rung,
                        rung.id,
                        format!("{} ({} kbps)", rung.name, rung.bitrate_kbps),
                    );
                }
            })
            .response
            .on_hover_text(
                "The rung the encoder assumes before real link feedback arrives, and prefers \
                 for newest-segment coverage whenever the link can sustain it — falling back \
                 to the low rung otherwise, so this never risks dropout. Takes effect next \
                 time you go live — no restart needed.",
            );
        if self.cfg.default_rung != before {
            self.persist_config();
        }

        if self.pending_rung_toggle.is_some() {
            self.rung_toggle_confirm_window(ui.ctx());
        }
    }

    /// Applies a rung enable/disable and persists it; if live, restarts the
    /// pipeline by resending `GoLive` with the freshly filtered profile —
    /// reuses the same stop+respawn path `toggle_live` already drives, so no
    /// new controller command is needed.
    fn apply_rung_toggle(&mut self, rung: RungId, enable: bool) {
        if enable {
            if !self.cfg.enabled_rungs.contains(&rung) {
                self.cfg.enabled_rungs.push(rung);
                self.cfg.enabled_rungs.sort_unstable();
            }
        } else if self.cfg.enabled_rungs.len() > 1 {
            self.cfg.enabled_rungs.retain(|&r| r != rung);
        }
        self.persist_config();
        if self.live {
            let cmd = self.go_live_cmd();
            let _ = self.cmd_tx.send(cmd);
        }
    }

    /// Confirmation dialog for a rung toggle clicked while live — see
    /// `pending_rung_toggle`.
    fn rung_toggle_confirm_window(&mut self, ctx: &egui::Context) {
        let Some((rung, enable)) = self.pending_rung_toggle else {
            return;
        };
        let name = StreamProfile::default_ladder(self.cfg.segment_ms)
            .rungs
            .iter()
            .find(|r| r.id == rung)
            .map(|r| r.name.clone())
            .unwrap_or_default();
        let verb = if enable { "Enable" } else { "Disable" };
        let mut apply = false;
        let mut cancel = false;
        egui::Window::new("Restart encoder?")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ctx, |ui| {
                ui.label(format!(
                    "{verb} rung \"{name}\" now? This briefly restarts the encoder pipeline \
                     (a few seconds of audio gap) to pick up the change."
                ));
                ui.horizontal(|ui| {
                    if ui.button("Apply & restart").clicked() {
                        apply = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });
        if apply {
            self.apply_rung_toggle(rung, enable);
        }
        if apply || cancel {
            self.pending_rung_toggle = None;
        }
    }

    fn meter_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Levels");
        let ((vu_l, ppm_l), (vu_r, ppm_r)) = self.audio.meters();
        let (peak_l, peak_r) = self.audio.peaks_db();
        let clipped_l = self.audio.take_clip_l();
        let clipped_r = self.audio.take_clip_r();
        let mono = self.audio.mono();

        ui.horizontal(|ui| {
            ui.label("Flying peak marker:");
            let mut mode = self.cfg.peak_mode;
            if ui
                .selectable_value(&mut mode, PeakMode::Ppm, "PPM")
                .clicked()
                || ui
                    .selectable_value(&mut mode, PeakMode::DigitalPeak, "dBFS peak")
                    .clicked()
            {
                self.cfg.peak_mode = mode;
                self.persist_config();
            }
        });

        let (peak_display_l, peak_display_r) = match self.cfg.peak_mode {
            PeakMode::Ppm => (ppm_l, ppm_r),
            PeakMode::DigitalPeak => (peak_l, peak_r),
        };

        ui.horizontal(|ui| {
            let reading_l = MeterReading {
                vu_db: vu_l,
                peak_db: peak_display_l,
                clipped: clipped_l,
            };
            let label_l = if mono { "MONO" } else { "L" };
            let resp_l = level_meter(ui, label_l, &reading_l, egui::vec2(130.0, 280.0));
            if resp_l.clicked() {
                self.audio.reset_clips();
            }

            if !mono {
                let reading_r = MeterReading {
                    vu_db: vu_r,
                    peak_db: peak_display_r,
                    clipped: clipped_r,
                };
                let resp_r = level_meter(ui, "R", &reading_r, egui::vec2(130.0, 280.0));
                if resp_r.clicked() {
                    self.audio.reset_clips();
                }
            }

            ui.add_space(16.0);
            ui.vertical(|ui| {
                ui.label(
                    egui::RichText::new(
                        "0 VU = -18 dBFS. Left scale: VU-relative. Right scale: dBFS.",
                    )
                    .small(),
                );
                ui.label(format!(
                    "L  vu {vu_l:>5.1} dB   pk {peak_display_l:>5.1} dB"
                ));
                if !mono {
                    ui.label(format!(
                        "R  vu {vu_r:>5.1} dB   pk {peak_display_r:>5.1} dB"
                    ));
                }
                if clipped_l || clipped_r {
                    ui.colored_label(
                        egui::Color32::from_rgb(0xff, 0x40, 0x40),
                        "CLIP — click a meter to clear",
                    );
                }
            });
        });

        ui.separator();
        ui.horizontal(|ui| {
            let (momentary, short_term, integrated) = self.audio.lufs();
            ui.label(egui::RichText::new("LUFS").strong());
            ui.label(format!("M {momentary:>6.1}"));
            ui.label(format!("S {short_term:>6.1}"));
            ui.label(format!("I {integrated:>6.1}"));
            if ui
                .small_button("Reset I")
                .on_hover_text(
                    "Clear the integrated (whole-programme) LUFS reading's gated history — \
                     momentary and short-term are unaffected",
                )
                .clicked()
            {
                self.audio.reset_integrated_lufs();
            }
        });
    }

    /// Link health at a glance: how much safety buffer is left, how hard
    /// we're leaning on the link, and what quality is actually reaching
    /// listeners right now. All three read off state already flowing
    /// through the closed loop (CLAUDE.md §1/§6) — nothing here is polled
    /// separately.
    fn link_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Link");

        let Some((state, age)) = self.shared.server_snapshot() else {
            ui.label("(no link yet)");
            return;
        };

        // Buffer: while stopped, the pre-roll accumulating toward the
        // auto-start target (if enabled) — otherwise just DVR depth. Once
        // playing, the lead ahead of the playout head, i.e. the safety
        // margin a network outage eats into first.
        let stopped = state.playout.state == PlayoutState::Stopped;
        let raw_buffer_ms = if stopped {
            state.buffered_ms
        } else {
            state.lead_ms
        };
        // Both `lead_ms` and `buffered_ms` only shrink from here in real
        // time once nothing new is arriving: while playing, playout keeps
        // consuming the lead with no upload replenishing it; while stopped,
        // the server's DVR window keeps evicting its oldest end while the
        // live edge sits frozen (no segments to extend it). So if the link
        // (and thus a fresh `ServerState`) has gone quiet, extrapolate that
        // drain from how long ago we last heard rather than freezing at the
        // last number reported — a stale connection would otherwise show a
        // deceptively healthy buffer through an outage. In normal operation
        // `age` stays near zero (state refreshes every tick), so this is a
        // no-op then and only kicks in once the feed actually stalls.
        let age_ms = age.as_millis().min(u32::MAX as u128) as u32;
        let buffer_ms = raw_buffer_ms.saturating_sub(age_ms);
        let buffer_target_ms = if stopped && self.cfg.auto_start {
            (self.cfg.auto_start_buffer_secs * 1000).max(1)
        } else {
            state.water.high_ms.max(1)
        };
        let buffer_frac = buffer_ms as f32 / buffer_target_ms as f32;
        let buffer_color = if buffer_ms < state.water.low_ms {
            egui::Color32::from_rgb(0xe2, 0x3d, 0x3d)
        } else if buffer_ms < state.water.target_ms {
            egui::Color32::from_rgb(0xe8, 0xc5, 0x2a)
        } else {
            egui::Color32::from_rgb(0x35, 0xc7, 0x5f)
        };
        let buffer_stale = age >= crate::shared::STALE_AFTER;
        let buffer_text = if stopped && self.cfg.auto_start {
            format!(
                "{:.0}s / {:.0}s buffered (auto-start pending)",
                buffer_ms as f32 / 1000.0,
                buffer_target_ms as f32 / 1000.0
            )
        } else if buffer_stale {
            format!(
                "{:.1} s (estimated — link down {:.0}s)",
                buffer_ms as f32 / 1000.0,
                age.as_secs_f32()
            )
        } else {
            format!("{:.1} s", buffer_ms as f32 / 1000.0)
        };
        ui.label("Buffer");
        ui.add(
            egui::ProgressBar::new(buffer_frac.clamp(0.0, 1.0))
                .fill(buffer_color)
                .text(buffer_text),
        );

        // Bandwidth: the bitrate the currently-prioritized rung needs,
        // against the link's last measured achievable throughput. 100% is
        // exactly the boundary where the link can just barely sustain it —
        // below that there's headroom for upgrades; at/above, the link is
        // the bottleneck (why we may be stuck in survival).
        let primary_kbps = self.profile().bitrate_of(self.shared.primary_rung()) as f32;
        let link_kbps = self.shared.throughput_kbps().max(1) as f32;
        let bandwidth_pct = primary_kbps / link_kbps * 100.0;
        ui.label("Bandwidth used");
        ui.add(
            egui::ProgressBar::new((bandwidth_pct / 100.0).clamp(0.0, 1.0))
                .text(format!("{bandwidth_pct:.0}% of link")),
        );

        // Quality on air: ground truth while connected, a best-effort guess
        // from our own upload history while the link's gone quiet — see
        // `SharedState::playing_quality`.
        let quality = self.shared.playing_quality(self.cfg.segment_ms);
        ui.label("Quality on air");
        match quality {
            Some(q) => {
                let name = self
                    .profile()
                    .rungs
                    .iter()
                    .find(|r| r.id == q.rung)
                    .map(|r| r.name.clone())
                    .unwrap_or_else(|| format!("rung {}", q.rung));
                let (text, color) = if q.estimated {
                    (
                        format!("{name} (estimated — link down)"),
                        egui::Color32::from_rgb(0xe8, 0xc5, 0x2a),
                    )
                } else {
                    (name, egui::Color32::from_rgb(0x35, 0xc7, 0x5f))
                };
                ui.colored_label(color, text);
            }
            None => {
                ui.label("(not playing)");
            }
        }

        // Buffer quality: the segments the server currently reports coverage
        // for ahead of the playout head (`ServerState::coverage` — "where
        // the quality holes are", CLAUDE.md §1), broken down by which rung
        // each one is actually held at — stacked so every enabled rung's
        // share is visible at once and the segments together always sum to
        // the full covered fraction. Gaps (`best_rung == None`) don't count
        // toward any segment — a missing segment is a continuity problem,
        // not a quality one (that's `Buffer`/continuity's job above).
        let rungs = self.profile().rungs.clone();
        let top_rung = self.profile().top_rung();
        let covered: Vec<_> = state.coverage.iter().filter_map(|c| c.best_rung).collect();
        let top_rung_pct = if covered.is_empty() {
            None
        } else {
            let hd = covered.iter().filter(|&&r| r == top_rung).count();
            Some(hd as f32 / covered.len() as f32 * 100.0)
        };
        ui.label("Buffer quality (by rung)");
        if covered.is_empty() {
            ui.label("(no buffer coverage yet)");
        } else {
            let total = covered.len() as f32;
            let segments: Vec<(f32, egui::Color32)> = rungs
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    let count = covered.iter().filter(|&&cr| cr == r.id).count();
                    (count as f32 / total, meter::rung_color(i, rungs.len()))
                })
                .collect();
            meter::stacked_bar(ui, &segments, egui::vec2(200.0, 18.0));
            ui.horizontal_wrapped(|ui| {
                for (i, r) in rungs.iter().enumerate() {
                    let count = covered.iter().filter(|&&cr| cr == r.id).count();
                    if count == 0 {
                        continue;
                    }
                    let pct = count as f32 / total * 100.0;
                    ui.colored_label(
                        meter::rung_color(i, rungs.len()),
                        format!("{} {pct:.0}%", r.name),
                    );
                }
            });
        }

        // Sample the rolling history at a fixed cadence, independent of the
        // ~30fps repaint (see `HISTORY_SAMPLE_INTERVAL`), then render it —
        // all four share the "last 60s" framing the buffer/bandwidth/
        // quality/buffer-quality readouts above are snapshots of.
        let now = Instant::now();
        if self
            .history_sampled_at
            .is_none_or(|t| now.duration_since(t) >= HISTORY_SAMPLE_INTERVAL)
        {
            self.history_sampled_at = Some(now);
            push_capped(
                &mut self.buffer_history,
                buffer_ms as f32 / 1000.0,
                HISTORY_LEN,
            );
            push_capped(&mut self.bandwidth_history, bandwidth_pct, HISTORY_LEN);
            if let Some(q) = quality {
                push_capped(&mut self.quality_history, q.rung as f32, HISTORY_LEN);
            }
            if let Some(pct) = top_rung_pct {
                push_capped(&mut self.buffer_quality_history, pct, HISTORY_LEN);
            }
        }

        ui.separator();
        ui.label(egui::RichText::new("Last 60s").small());

        ui.label(egui::RichText::new("Buffer (s)").small());
        meter::sparkline(
            ui,
            &self.buffer_history,
            0.0,
            (buffer_target_ms as f32 / 1000.0).max(1.0),
            egui::vec2(200.0, 44.0),
            buffer_color,
        );

        ui.label(egui::RichText::new("Bandwidth (% of link)").small());
        meter::sparkline(
            ui,
            &self.bandwidth_history,
            0.0,
            150.0,
            egui::vec2(200.0, 44.0),
            egui::Color32::from_rgb(0x5b, 0x8f, 0xc9),
        );

        let top_rung_f = self.profile().top_rung().max(1) as f32;
        ui.label(egui::RichText::new("Quality (rung, low→high)").small());
        meter::sparkline(
            ui,
            &self.quality_history,
            0.0,
            top_rung_f,
            egui::vec2(200.0, 44.0),
            egui::Color32::from_rgb(0xc9, 0x8f, 0x5b),
        );

        ui.label(egui::RichText::new("Buffer quality trend (% at top rung)").small());
        meter::sparkline(
            ui,
            &self.buffer_quality_history,
            0.0,
            100.0,
            egui::vec2(200.0, 44.0),
            egui::Color32::from_rgb(0x35, 0xc7, 0x5f),
        );
    }

    /// Scrollable operator status/error log — see `SharedState::recent_log`.
    /// Shown as a collapsible bottom panel (toggled from the status bar)
    /// rather than always-on, so it doesn't compete for space with the
    /// meters during normal operation but stays one click away.
    fn log_panel(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Log");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.small_button("Close").clicked() {
                    self.show_log = false;
                }
            });
        });
        ui.separator();

        let entries = self.shared.recent_log();
        egui::ScrollArea::vertical()
            .stick_to_bottom(true)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                if entries.is_empty() {
                    ui.label("(no log entries yet)");
                }
                for entry in &entries {
                    log_line(ui, entry);
                }
            });
    }

    /// Extrapolated buffer depth (ms) and whether the link is currently down
    /// (`age >= STALE_AFTER`) — the same numbers `link_panel`'s "Buffer"
    /// readout is built from, factored out here so the connection-fail
    /// flash agrees with it rather than a second, potentially-drifting copy
    /// of the same formula.
    fn buffer_estimate(&self) -> Option<(u32, bool)> {
        let (state, age) = self.shared.server_snapshot()?;
        let stopped = state.playout.state == PlayoutState::Stopped;
        let raw_buffer_ms = if stopped {
            state.buffered_ms
        } else {
            state.lead_ms
        };
        let age_ms = age.as_millis().min(u32::MAX as u128) as u32;
        let buffer_ms = raw_buffer_ms.saturating_sub(age_ms);
        Some((buffer_ms, age >= crate::shared::STALE_AFTER))
    }

    /// The window-border flash color for this frame, if any. A fresh clip on
    /// the selected inputs (edge-detected — see `was_clipping`) takes
    /// priority and flashes `CLIP_FLASH_COUNT` times, since it's a discrete
    /// event that would otherwise get masked by an ongoing link outage.
    /// Once that's done (or never started), a currently-down link flashes
    /// continuously for as long as the outage lasts — yellow while `Buffer`
    /// still has `FLASH_BUFFER_THRESHOLD_MS` of headroom, red once it's
    /// below that and dropout is close.
    ///
    /// Both alarms are also logged (`SharedState::push_log`, edge-triggered
    /// so an ongoing outage doesn't spam a line every frame) so the status
    /// bar's infobar picks them up in the alert's own color — see
    /// `status_bar`'s `log_entry`. Logging never forces `show_log` open;
    /// the operator log panel only opens when the operator clicks "Log".
    fn flash_color(&mut self, now: Instant) -> Option<egui::Color32> {
        let clipping = self.audio.take_clip_l() || self.audio.take_clip_r();
        if clipping && !self.was_clipping {
            self.clip_flash_started_at = Some(now);
            self.shared.push_log(LogLevel::Warn, "input clip detected");
        }
        self.was_clipping = clipping;

        if let Some(started) = self.clip_flash_started_at {
            let elapsed = now.duration_since(started).as_secs_f32();
            let half_period = FLASH_HALF_PERIOD.as_secs_f32();
            let half_periods_elapsed = (elapsed / half_period) as u32;
            if half_periods_elapsed < CLIP_FLASH_COUNT * 2 {
                let on = half_periods_elapsed.is_multiple_of(2);
                return on.then_some(egui::Color32::from_rgb(0xff, 0x40, 0x40));
            }
            self.clip_flash_started_at = None;
        }

        let buffer_estimate = self.buffer_estimate();
        let link_down = matches!(buffer_estimate, Some((_, true)));
        if link_down && !self.was_link_down {
            self.shared.push_log(LogLevel::Warn, "server link down");
        }
        if !link_down {
            self.link_down_logged_critical = false;
        }
        self.was_link_down = link_down;

        let (buffer_ms, _) = buffer_estimate?;
        if !link_down {
            return None;
        }
        let critical = buffer_ms < FLASH_BUFFER_THRESHOLD_MS;
        if critical && !self.link_down_logged_critical {
            self.shared.push_log(
                LogLevel::Error,
                format!("server link down and buffer critical ({buffer_ms} ms left)"),
            );
            self.link_down_logged_critical = true;
        }
        let color = if critical {
            egui::Color32::from_rgb(0xe2, 0x3d, 0x3d)
        } else {
            egui::Color32::from_rgb(0xe8, 0xc5, 0x2a)
        };
        let elapsed = now.duration_since(self.created_at).as_secs_f32();
        let half_periods_elapsed = (elapsed / FLASH_HALF_PERIOD.as_secs_f32()) as u64;
        half_periods_elapsed.is_multiple_of(2).then_some(color)
    }

    /// Paints the blinking border (see `flash_color`) over everything else,
    /// on the foreground layer, so it's visible regardless of which panel
    /// has focus.
    fn draw_flash_overlay(&mut self, ui: &mut egui::Ui) {
        let now = Instant::now();
        let Some(color) = self.flash_color(now) else {
            return;
        };
        let rect = ui.max_rect();
        let painter = ui.ctx().layer_painter(egui::LayerId::new(
            egui::Order::Foreground,
            egui::Id::new("flash_overlay"),
        ));
        painter.rect_stroke(
            rect,
            0.0,
            egui::Stroke::new(10.0, color),
            egui::StrokeKind::Inside,
        );
    }
}

impl eframe::App for ObcastApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Meters need to visibly move; repaint at ~30fps regardless of input.
        ui.ctx().request_repaint_after(Duration::from_millis(33));

        // If the encoder pipeline died on its own, drop back out of "live" so
        // the button and status reflect reality (the error text stays visible
        // in the status bar's log summary and the full log panel).
        if self.live && self.shared.take_encoder_failed() {
            self.live = false;
        }

        egui::Panel::top("top").show(ui, |ui| {
            ui.add_space(4.0);
            self.status_bar(ui);
            ui.add_space(4.0);
        });

        if self.show_log {
            egui::Panel::bottom("log")
                .resizable(true)
                .default_size(220.0)
                .min_size(120.0)
                .show(ui, |ui| {
                    self.log_panel(ui);
                });
        }

        egui::Panel::left("controls")
            .min_size(360.0)
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    self.device_panel(ui);
                });
            });

        egui::Panel::right("link").min_size(240.0).show(ui, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                self.link_panel(ui);
            });
        });

        egui::CentralPanel::default().show(ui, |ui| {
            self.meter_panel(ui);
        });

        self.draw_flash_overlay(ui);
    }
}

/// One row of the log panel: wall-clock time, a color-coded level tag, and
/// the message.
fn log_line(ui: &mut egui::Ui, entry: &LogEntry) {
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(format_log_time(entry.at_ms))
                .monospace()
                .small()
                .color(egui::Color32::GRAY),
        );
        ui.colored_label(
            log_level_color(entry.level),
            egui::RichText::new(log_level_tag(entry.level))
                .monospace()
                .small()
                .strong(),
        );
        ui.label(&entry.message);
    });
}

/// `HH:MM:SS`, UTC-based (epoch millis are wall-clock, but converting to the
/// operator's local timezone would need a `chrono`/`time` dependency this
/// dependency-light crate doesn't otherwise carry — see CLAUDE.md §9).
fn format_log_time(at_ms: u64) -> String {
    let secs = at_ms / 1000;
    let day_secs = secs % 86_400;
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn log_level_color(level: LogLevel) -> egui::Color32 {
    match level {
        LogLevel::Error => egui::Color32::from_rgb(0xe2, 0x3d, 0x3d),
        LogLevel::Warn => egui::Color32::from_rgb(0xe8, 0xc5, 0x2a),
        LogLevel::Info => egui::Color32::LIGHT_GRAY,
    }
}

fn log_level_tag(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "ERROR",
        LogLevel::Warn => "WARN ",
        LogLevel::Info => "INFO ",
    }
}

/// Append a Link-panel history sample, dropping the oldest once past `cap`.
fn push_capped(history: &mut VecDeque<f32>, value: f32, cap: usize) {
    history.push_back(value);
    while history.len() > cap {
        history.pop_front();
    }
}

/// A live PCM feed must produce a block at least this often (in units of
/// `segment_ms`) or the pipeline is considered stalled. Mirrors the bound
/// already used for a permanent continuity gap elsewhere in this codebase
/// (`playout.rs`'s `3 * segment_ms` skip-ahead backstop, `uploader.rs`'s
/// `ABANDON_AFTER`) rather than inventing a new one.
const PCM_STALL_MULTIPLIER: u32 = 3;

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

    // Tracks whether audio capture is actually still flowing while live.
    // Unlike a broken ffmpeg pipe (which fails loudly on the next stdin
    // write, see the `pcm_rx.recv()` arm below), a lost input device can
    // simply stop invoking cpal's data callback with no error at all — the
    // `pcm_rx` channel then just goes quiet forever, ffmpeg's stdin never
    // sees a write (so never errors), and nothing ever told the operator.
    // This watchdog is what catches that silent case.
    let mut segment_ms: u32 = 0;
    let mut last_pcm_at: Option<tokio::time::Instant> = None;
    let mut watchdog = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ControllerCmd::GoLive { profile, base_url, stream, ingest_token, out_dir, sample_rate, auto_start_buffer_ms, bootstrap_rung }) => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;

                        if let Err(err) = tokio::fs::create_dir_all(&out_dir).await {
                            tracing::error!(error = %err, "failed to create buffer dir");
                            shared.push_log(LogLevel::Error, format!("failed to create buffer dir: {err}"));
                            audio.set_live(false);
                            continue;
                        }

                        segment_ms = profile.segment_ms;
                        match encode::spawn(&encode::Source::Pcm { sample_rate }, &profile, &out_dir) {
                            Ok((mut c, warnings)) => {
                                for warning in &warnings {
                                    tracing::warn!(%warning, "codec fallback");
                                    shared.push_log(LogLevel::Warn, warning.clone());
                                }
                                stdin = c.stdin.take();
                                child = Some(c);
                                audio.set_live(true);
                                // Seed so the watchdog counts from go-live,
                                // not from whenever the first block happens
                                // to land.
                                last_pcm_at = Some(tokio::time::Instant::now());

                                let client = reqwest::Client::new();
                                sse_handle = Some(tokio::spawn(sse::run(
                                    client.clone(),
                                    base_url.clone(),
                                    stream.clone(),
                                    shared.clone(),
                                )));
                                upload_handle = Some(tokio::spawn(uploader::run(
                                    client,
                                    uploader::Config { base_url, stream, ingest_token, out_dir, profile, auto_start_buffer_ms, bootstrap_rung },
                                    shared.clone(),
                                )));
                                tracing::info!("live: encoder pipeline started");
                                shared.push_log(LogLevel::Info, "live: encoder pipeline started");
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "failed to spawn ffmpeg");
                                shared.push_log(LogLevel::Error, format!("failed to start ffmpeg: {err}"));
                                audio.set_live(false);
                            }
                        }
                    }
                    Some(ControllerCmd::StopLive) => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
                        tracing::info!("live: encoder pipeline stopped");
                        shared.push_log(LogLevel::Info, "live: encoder pipeline stopped");
                    }
                    None => {
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
                        return;
                    }
                }
            }
            pcm = pcm_rx.recv() => {
                let Some(block) = pcm else { return };
                // A block arrived, so capture is alive regardless of what
                // happens to it downstream — this is what the watchdog below
                // checks for staleness.
                last_pcm_at = Some(tokio::time::Instant::now());
                let mut write_err = None;
                if let Some(s) = stdin.as_mut() {
                    let mut bytes = Vec::with_capacity(block.len() * 4);
                    for sample in &block {
                        bytes.extend_from_slice(&sample.to_le_bytes());
                    }
                    if let Err(err) = s.write_all(&bytes).await {
                        write_err = Some(err);
                    }
                }
                // A failed stdin write almost always means ffmpeg already
                // exited (broken pipe). Rather than silently dropping the feed
                // and leaving the GUI showing a dead "live", tear the pipeline
                // down and surface the reason so the operator can re-Go-Live.
                if let Some(err) = write_err {
                    let detail = match child.as_mut().and_then(|c| c.try_wait().ok().flatten()) {
                        Some(status) => format!("ffmpeg exited ({status}); live stopped"),
                        None => format!("ffmpeg stdin write failed: {err}; live stopped"),
                    };
                    tracing::error!(error = %detail, "encoder pipeline died");
                    shared.push_log(LogLevel::Error, detail);
                    stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
                }
            }
            _ = watchdog.tick() => {
                if child.is_some() {
                    let stalled = last_pcm_at
                        .is_some_and(|t| t.elapsed() >= Duration::from_millis((segment_ms as u64) * PCM_STALL_MULTIPLIER as u64));
                    if stalled {
                        let detail = match audio.last_error() {
                            Some(err) => format!("audio capture stopped ({err}); live stopped"),
                            None => "no audio received from capture device; live stopped".to_string(),
                        };
                        tracing::error!(error = %detail, "encoder pipeline stalled: no PCM from capture device");
                        shared.push_log(LogLevel::Error, detail);
                        stop_live(&mut stdin, &mut child, &mut sse_handle, &mut upload_handle, &audio).await;
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
