//! # bc-ble-core
//!
//! Shared BLE core library for BrainCo Rust SDKs.
//!
//! This crate provides the common BLE transport layer used by:
//! - `bc-band-sdk` (Myon EEG headband)
//! - `honda-sdk` (Honda EEG headband)
//! - `morpheus-sdk` (Morpheus/Melody sleep devices)
//!
//! ## Architecture
//!
//! The crate is split into layers:
//! - **types** — shared data types (ScanResult, ConnectionState, BLEDeviceInfo)
//! - **runtime** — tokio runtime helpers (block_on_any, spawn_any)
//! - **callbacks** — callback storage and dispatch
//! - **adapter** — central adapter initialization and event listening
//! - **scan** — scan start/stop with Windows UUID filter
//! - **connect** — connect/disconnect, MTU, service discovery, notifications
//! - **write** — BLE write with MTU chunking
//! - **c_utils** — C FFI utilities (PeripheralId conversion, UUID parsing)
//! - **constants** — standard BLE UUIDs (battery, device info)

pub mod adapter;
pub mod c_utils;
pub mod callbacks;
pub mod connect;
pub mod constants;
pub mod runtime;
pub mod scan;
pub mod types;
pub mod write;

/// Re-export btleplug types commonly needed by downstream crates.
pub mod prelude {
    pub use crate::adapter::*;
    pub use crate::callbacks::*;
    pub use crate::constants::*;
    pub use crate::runtime::*;
    pub use crate::scan::*;
    pub use crate::types::*;
    pub use crate::write::*;

    pub use btleplug::api::CentralState;
    pub use uuid::Uuid;
}

/// Re-export btleplug for downstream crates that need direct access.
pub use btleplug;
pub use uuid;
