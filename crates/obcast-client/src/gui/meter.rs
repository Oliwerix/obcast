//! A K-14 style loudness meter widget for `egui`.
//!
//! K-System metering (Bob Katz) puts a "0" reference well below digital
//! full scale, leaving headroom for transients instead of riding the
//! ceiling — K-14 leaves 14 dB, the broadcast convention. This widget
//! draws that scale directly (ticks labelled in K-relative dB), fills a
//! bar to the IEC 60268-17 VU (loudness) level colour-zoned
//! green/yellow/red around the K-0 point, and overlays the IEC 60268-10
//! PPM (fast peak) reading as a "flying" marker so a transient is still
//! readable after it's gone — both ballistics computed on the audio thread.

use eframe::egui::{self, Color32, Rect, Stroke, Vec2};

/// K-14: "0" on the K scale sits at -14 dBFS.
const K_OFFSET: f32 = 14.0;
const MIN_DBFS: f32 = -50.0;
const MAX_DBFS: f32 = 0.0;

const GREEN: Color32 = Color32::from_rgb(0x35, 0xc7, 0x5f);
const YELLOW: Color32 = Color32::from_rgb(0xe8, 0xc5, 0x2a);
const RED: Color32 = Color32::from_rgb(0xe2, 0x3d, 0x3d);
const TRACK: Color32 = Color32::from_rgb(0x20, 0x22, 0x26);

pub fn linear_to_dbfs(linear: f32) -> f32 {
    if linear <= 0.0003 {
        -100.0
    } else {
        20.0 * linear.log10()
    }
}

fn frac(dbfs: f32) -> f32 {
    ((dbfs.clamp(MIN_DBFS, MAX_DBFS) - MIN_DBFS) / (MAX_DBFS - MIN_DBFS)).clamp(0.0, 1.0)
}

fn zone_color(dbfs: f32) -> Color32 {
    let k = dbfs + K_OFFSET;
    if k < 0.0 {
        GREEN
    } else if k < 4.0 {
        YELLOW
    } else {
        RED
    }
}

pub struct MeterReading {
    /// IEC 60268-17 VU — the slow "loudness" bar.
    pub vu_db: f32,
    /// IEC 60268-10 PPM — the fast "flying" peak marker.
    pub ppm_db: f32,
    pub clipped: bool,
}

/// One vertical K-14 channel strip. `size` should leave ~28px on the right
/// for the scale ruler. Returns a clickable response (callers wire clicks
/// to a clip-LED reset).
pub fn k14_meter(
    ui: &mut egui::Ui,
    label: &str,
    reading: &MeterReading,
    size: Vec2,
) -> egui::Response {
    ui.vertical(|ui| {
        ui.label(
            egui::RichText::new(label)
                .size(11.0)
                .color(Color32::LIGHT_GRAY),
        );

        let (rect, response) = ui.allocate_exact_size(size, egui::Sense::click());
        let painter = ui.painter_at(rect);

        painter.rect_filled(rect, 3.0, TRACK);

        let bar_w = (rect.width() * 0.5).max(10.0);
        let bar_rect = Rect::from_min_size(
            egui::pos2(rect.left(), rect.top() + 8.0),
            Vec2::new(bar_w, rect.height() - 8.0),
        );

        // Faint always-visible zone guide so the scale reads at any level.
        for (lo, hi, color) in [
            (MIN_DBFS, -K_OFFSET, GREEN),
            (-K_OFFSET, -K_OFFSET + 4.0, YELLOW),
            (-K_OFFSET + 4.0, MAX_DBFS, RED),
        ] {
            let y0 = bar_rect.bottom() - frac(hi) * bar_rect.height();
            let y1 = bar_rect.bottom() - frac(lo) * bar_rect.height();
            painter.rect_filled(
                Rect::from_min_max(
                    egui::pos2(bar_rect.left(), y0),
                    egui::pos2(bar_rect.right(), y1),
                ),
                0.0,
                color.gamma_multiply(0.12),
            );
        }

        // VU fill — the IEC 60268-17 loudness reading, the main thing
        // K-metering is for.
        let vu_frac = frac(reading.vu_db);
        let fill_top = bar_rect.bottom() - vu_frac * bar_rect.height();
        painter.rect_filled(
            Rect::from_min_max(egui::pos2(bar_rect.left(), fill_top), bar_rect.max),
            0.0,
            zone_color(reading.vu_db),
        );

        // PPM marker — the IEC 60268-10 fast-attack/slow-decay peak needle,
        // "flying" above the slower VU bar. Its ballistics come from the audio
        // thread, so no display-side hold/decay is needed here.
        let ppm_y = bar_rect.bottom() - frac(reading.ppm_db) * bar_rect.height();
        painter.line_segment(
            [
                egui::pos2(bar_rect.left(), ppm_y),
                egui::pos2(bar_rect.right(), ppm_y),
            ],
            Stroke::new(2.0, Color32::from_rgb(0xf5, 0xf5, 0xf5)),
        );

        // K-scale ruler, labelled relative to the K-14 "0" (i.e. -14 dBFS).
        for k in [-36i32, -26, -16, -6, 0, 4, 7, 10, 14] {
            let dbfs = k as f32 - K_OFFSET;
            let y = bar_rect.bottom() - frac(dbfs) * bar_rect.height();
            painter.line_segment(
                [
                    egui::pos2(bar_rect.right() + 2.0, y),
                    egui::pos2(bar_rect.right() + 5.0, y),
                ],
                Stroke::new(1.0, Color32::GRAY),
            );
            painter.text(
                egui::pos2(bar_rect.right() + 7.0, y),
                egui::Align2::LEFT_CENTER,
                k.to_string(),
                egui::FontId::monospace(8.0),
                Color32::GRAY,
            );
        }

        // Clip LED across the top; click the meter to reset it.
        let led_rect = Rect::from_min_size(rect.min, Vec2::new(bar_rect.width(), 6.0));
        painter.rect_filled(
            led_rect,
            2.0,
            if reading.clipped {
                Color32::from_rgb(0xff, 0x30, 0x30)
            } else {
                TRACK
            },
        );

        response
    })
    .inner
}

/// Compact horizontal peak meter for a channel-bank row — enough to spot
/// which of e.g. 32 inputs actually has signal on it, without the full
/// K-scale ruler.
pub fn mini_meter(ui: &mut egui::Ui, peak_linear: f32, size: Vec2) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 2.0, TRACK);

    let dbfs = linear_to_dbfs(peak_linear);
    let w = frac(dbfs) * rect.width();
    if w > 0.5 {
        let fill = Rect::from_min_size(rect.min, Vec2::new(w, rect.height()));
        painter.rect_filled(fill, 2.0, zone_color(dbfs));
    }
    response
}
