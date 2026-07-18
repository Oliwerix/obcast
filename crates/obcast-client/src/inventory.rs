//! Scans the local segment ring buffer to build the `LocalInventory` the
//! scheduler needs. Pure filesystem listing — no ffmpeg awareness beyond the
//! naming convention it writes (`{rung}/{seq}.ts`).

use std::collections::BTreeMap;
use std::path::Path;

use obcast_proto::state::{RungId, Seq};

pub struct ScanResult {
    pub oldest_seq: Seq,
    pub encoded_seq: Seq,
    pub available: BTreeMap<Seq, Vec<RungId>>,
}

/// For each rung directory, every file except the highest-numbered one is
/// finalized — ffmpeg is still writing to the current segment, so treating it
/// as available would upload a truncated file.
pub fn scan(out_dir: &Path, rungs: &[RungId]) -> ScanResult {
    let mut available: BTreeMap<Seq, Vec<RungId>> = BTreeMap::new();
    let mut oldest = u64::MAX;
    let mut newest = 0u64;

    for &rung in rungs {
        let dir = out_dir.join(rung.to_string());
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut seqs: Vec<Seq> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".ts"))
                    .and_then(|n| n.parse::<Seq>().ok())
            })
            .collect();
        seqs.sort_unstable();

        if seqs.len() > 1 {
            for &seq in &seqs[..seqs.len() - 1] {
                available.entry(seq).or_default().push(rung);
                oldest = oldest.min(seq);
                newest = newest.max(seq);
            }
        }
    }

    ScanResult {
        oldest_seq: if oldest == u64::MAX { 0 } else { oldest },
        encoded_seq: newest,
        available,
    }
}
