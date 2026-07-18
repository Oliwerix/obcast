//! Shared wire types and the closed-loop upload scheduler for OBCast.
//!
//! Pure and dependency-light (serde only) per CLAUDE.md — no I/O, no async.

pub mod control;
pub mod meter;
pub mod scheduler;
pub mod state;
