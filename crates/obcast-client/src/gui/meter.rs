//! A dual-scale VU/PPM loudness meter widget for `egui`.
//!
//! The "0" reference sits at -18 dBFS (EBU R68 analog line-up level) rather
//! than riding the digital ceiling, leaving headroom for transients. The
//! same fill bar carries two rulers read from opposite sides: the left edge
//! labels are VU-relative (0 at -18 dBFS), the right edge labels are plain
//! dBFS (0 at the top) — same tick grid, two ways to read it. The bar fills
//! to the IEC 60268-17 VU (loudness) level, colour-zoned green/yellow/red
//! around the 0 VU point, and overlays a "flying" peak marker — either the
//! IEC 60268-10 PPM ballistic or the raw true digital peak, operator's
//! choice — so a transient is still readable after it's gone. Both
//! ballistics are computed on the audio thread; this widget only draws.

use eframe::egui::{self, Color32, Rect, Stroke, Vec2};

/// 0 VU sits at -18 dBFS (EBU R68 line-up level).
const VU_REF_DBFS: f32 = -18.0;
/// Width of the yellow zone above 0 VU before it's considered red.
const YELLOW_SPAN_DB: f32 = 4.0;
const RED_THRESHOLD_DBFS: f32 = VU_REF_DBFS + YELLOW_SPAN_DB;

const MIN_DBFS: f32 = -50.0;
const MAX_DBFS: f32 = 0.0;

const GREEN: Color32 = Color32::from_rgb(0x35, 0xc7, 0x5f);
const YELLOW: Color32 = Color32::from_rgb(0xe8, 0xc5, 0x2a);
const RED: Color32 = Color32::from_rgb(0xe2, 0x3d, 0x3d);
const TRACK: Color32 = Color32::from_rgb(0x20, 0x22, 0x26);
/// Flat, non-zone-coded fill for the "all input channels" overview strip —
/// that view is for spotting which physical channel has signal, not for
/// reading levels, so it deliberately doesn't carry the VU zone colours.
const NEUTRAL_FILL: Color32 = Color32::from_rgb(0x5b, 0x8f, 0xc9);

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

/// The three VU zones as `(low_dbfs, high_dbfs, color)`, low→high.
fn zones() -> [(f32, f32, Color32); 3] {
    [
        (MIN_DBFS, VU_REF_DBFS, GREEN),
        (VU_REF_DBFS, RED_THRESHOLD_DBFS, YELLOW),
        (RED_THRESHOLD_DBFS, MAX_DBFS, RED),
    ]
}

pub struct MeterReading {
    /// IEC 60268-17 VU — the slow "loudness" bar.
    pub vu_db: f32,
    /// The flying peak marker reading — either IEC 60268-10 PPM or true
    /// digital peak, whichever the operator has selected. The widget just
    /// draws whatever's handed to it.
    pub peak_db: f32,
    pub clipped: bool,
}

/// One vertical dual-scale channel strip. `size` should be at least ~130px
/// wide to leave room for both rulers. Returns a clickable response (callers
/// wire clicks to a clip-LED reset).
pub fn level_meter(
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

        const LEFT_RULER_W: f32 = 30.0;
        const RIGHT_RULER_W: f32 = 26.0;
        let bar_left = rect.left() + LEFT_RULER_W;
        let bar_right = (rect.right() - RIGHT_RULER_W).max(bar_left + 10.0);
        let bar_rect = Rect::from_min_max(
            egui::pos2(bar_left, rect.top() + 8.0),
            egui::pos2(bar_right, rect.bottom()),
        );

        // Faint always-visible zone guide so the scale reads at any level.
        for (lo, hi, color) in zones() {
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

        // VU fill — the IEC 60268-17 loudness reading. Normally only the
        // portion of the fill actually sitting in the yellow/red zone is
        // drawn in that zone's colour (the green base stays green under a
        // yellow cap), so a brief nudge into yellow doesn't repaint the
        // whole bar. Crossing into red is the one exception: the entire
        // fill flips solid red as an unambiguous "too hot" signal.
        let fill_top = bar_rect.bottom() - frac(reading.vu_db) * bar_rect.height();
        let fill_rect = Rect::from_min_max(egui::pos2(bar_rect.left(), fill_top), bar_rect.max);
        if reading.vu_db >= RED_THRESHOLD_DBFS {
            painter.rect_filled(fill_rect, 0.0, RED);
        } else {
            for (lo, hi, color) in zones() {
                let seg_hi = hi.min(reading.vu_db);
                if seg_hi <= lo {
                    continue;
                }
                let y0 = bar_rect.bottom() - frac(seg_hi) * bar_rect.height();
                let y1 = bar_rect.bottom() - frac(lo) * bar_rect.height();
                painter.rect_filled(
                    Rect::from_min_max(
                        egui::pos2(bar_rect.left(), y0),
                        egui::pos2(bar_rect.right(), y1),
                    ),
                    0.0,
                    color,
                );
            }
        }

        // Flying peak marker — "PPM" or "dBFS peak" per the caller's choice,
        // both computed on the audio thread, so no display-side hold/decay
        // is needed here.
        let peak_y = bar_rect.bottom() - frac(reading.peak_db) * bar_rect.height();
        painter.line_segment(
            [
                egui::pos2(bar_rect.left(), peak_y),
                egui::pos2(bar_rect.right(), peak_y),
            ],
            Stroke::new(2.0, Color32::from_rgb(0xf5, 0xf5, 0xf5)),
        );

        // Shared tick grid, read two ways: left edge in VU-relative dB
        // (0 = -18 dBFS), right edge in plain dBFS (0 at the top).
        let mut dbfs_tick = MAX_DBFS;
        while dbfs_tick >= MIN_DBFS {
            let y = bar_rect.bottom() - frac(dbfs_tick) * bar_rect.height();

            painter.line_segment(
                [
                    egui::pos2(bar_rect.left() - 5.0, y),
                    egui::pos2(bar_rect.left() - 2.0, y),
                ],
                Stroke::new(1.0, Color32::GRAY),
            );
            let vu_relative = dbfs_tick - VU_REF_DBFS;
            let vu_label = if vu_relative == 0.0 {
                "0".to_string()
            } else {
                format!("{vu_relative:+.0}")
            };
            painter.text(
                egui::pos2(bar_rect.left() - 7.0, y),
                egui::Align2::RIGHT_CENTER,
                vu_label,
                egui::FontId::monospace(8.0),
                Color32::GRAY,
            );

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
                format!("{dbfs_tick:.0}"),
                egui::FontId::monospace(8.0),
                Color32::GRAY,
            );

            dbfs_tick -= 6.0;
        }

        // Ruler headers, above the tick grid.
        painter.text(
            egui::pos2(bar_rect.left() - 2.0, rect.top()),
            egui::Align2::RIGHT_TOP,
            "VU",
            egui::FontId::monospace(8.0),
            Color32::DARK_GRAY,
        );
        painter.text(
            egui::pos2(bar_rect.right() + 2.0, rect.top()),
            egui::Align2::LEFT_TOP,
            "dBFS",
            egui::FontId::monospace(8.0),
            Color32::DARK_GRAY,
        );

        // Clip LED across the top; click the meter to reset it.
        let led_rect = Rect::from_min_size(
            egui::pos2(bar_rect.left(), rect.top()),
            Vec2::new(bar_rect.width(), 6.0),
        );
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
/// dual-scale ruler. Deliberately not zone-coloured (green/yellow/red) —
/// this view is for finding signal, not reading levels, and per-channel
/// colour coding here read as false level warnings on channels nobody had
/// assigned to L/R yet.
pub fn mini_meter(ui: &mut egui::Ui, peak_linear: f32, size: Vec2) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 2.0, TRACK);

    let dbfs = linear_to_dbfs(peak_linear);
    let w = frac(dbfs) * rect.width();
    if w > 0.5 {
        let fill = Rect::from_min_size(rect.min, Vec2::new(w, rect.height()));
        painter.rect_filled(fill, 2.0, NEUTRAL_FILL);
    }
    response
}
