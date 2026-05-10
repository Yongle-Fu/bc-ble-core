//! BLE callback storage and dispatch.
//!
//! This module provides the generic (Rust-side) callback infrastructure.
//! C FFI callback wrappers remain in each downstream SDK's `core_c.rs`.

use crate::types::*;
use btleplug::api::CentralState;
use btleplug::platform::PeripheralId;
use parking_lot::RwLock;

// ==================== Callback type definitions ====================

pub type AdapterStateCallback = Box<dyn Fn(CentralState) + Send + Sync>;
pub type ScanResultCallback = Box<dyn Fn(ScanResult) + Send + Sync>;
pub type ConnectionStateCallback = Box<dyn Fn(String, ConnectionState) + Send + Sync>;
pub type BatteryLevelCallback = Box<dyn Fn(String, u8) + Send + Sync>;
pub type ReceivedDataCallback = Box<dyn Fn(String, Vec<u8>) + Send + Sync>;

// ==================== Global callback storage ====================

lazy_static::lazy_static! {
  pub(crate) static ref ADAPTER_STATE_CALLBACK: RwLock<Option<AdapterStateCallback>> = RwLock::new(None);
  pub(crate) static ref DEVICE_DISCOVERED_CALLBACK: RwLock<Option<ScanResultCallback>> = RwLock::new(None);
  pub(crate) static ref SCAN_RESULT_CALLBACK: RwLock<Option<ScanResultCallback>> = RwLock::new(None);
  pub(crate) static ref CONNECTION_STATE_CALLBACK: RwLock<Option<ConnectionStateCallback>> = RwLock::new(None);
  pub(crate) static ref RECEIVED_DATA_CALLBACK: RwLock<Option<ReceivedDataCallback>> = RwLock::new(None);
  pub(crate) static ref BATTERY_LEVEL_CALLBACK: RwLock<Option<BatteryLevelCallback>> = RwLock::new(None);
}

// ==================== Adapter state ====================

pub fn set_adapter_state_callback(callback: AdapterStateCallback) {
    *ADAPTER_STATE_CALLBACK.write() = Some(callback);
}

pub fn run_adapter_state_callback(state: CentralState) {
    log::info!("run_adapter_state_callback: {state:?}");
    let cb = ADAPTER_STATE_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(state);
    } else {
        log::error!("Adapter state callback is not set");
    }
}

// ==================== Device discovered (DeviceDiscovered + DeviceUpdated) ====================

pub fn set_device_discovered_callback(callback: ScanResultCallback) {
    *DEVICE_DISCOVERED_CALLBACK.write() = Some(callback);
}

pub fn clear_device_discovered_callback() {
    *DEVICE_DISCOVERED_CALLBACK.write() = None;
}

pub(crate) fn is_registered_device_discovered() -> bool {
    DEVICE_DISCOVERED_CALLBACK.read().is_some()
}

pub(crate) fn run_device_discovered_callback(result: ScanResult) {
    let cb = DEVICE_DISCOVERED_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(result);
    }
}

// ==================== Scan result (ManufacturerDataAdvertisement) ====================

pub fn set_scan_result_callback(callback: ScanResultCallback) {
    *SCAN_RESULT_CALLBACK.write() = Some(callback);
}

pub fn clear_scan_result_callback() {
    *SCAN_RESULT_CALLBACK.write() = None;
}

pub(crate) fn is_registered_scan_result() -> bool {
    SCAN_RESULT_CALLBACK.read().is_some()
}

pub(crate) fn run_scan_result_callback(result: ScanResult) {
    let cb = SCAN_RESULT_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(result);
    }
}

// ==================== Connection state ====================

pub fn set_connection_state_callback(callback: ConnectionStateCallback) {
    *CONNECTION_STATE_CALLBACK.write() = Some(callback);
}

pub(crate) fn run_connection_state_callback(id: &PeripheralId, state: ConnectionState) {
    log::debug!("run_connection_state_callback: {id}, {state:?}");
    let cb = CONNECTION_STATE_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(id.to_string(), state);
    }
}

// ==================== Received data ====================

pub fn set_received_data_callback(callback: ReceivedDataCallback) {
    *RECEIVED_DATA_CALLBACK.write() = Some(callback);
}

pub(crate) fn run_received_data_callback(id: &PeripheralId, data: Vec<u8>) {
    let cb = RECEIVED_DATA_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(id.to_string(), data);
    }
}

// ==================== Battery level ====================

pub fn set_battery_level_callback(callback: BatteryLevelCallback) {
    *BATTERY_LEVEL_CALLBACK.write() = Some(callback);
}

pub(crate) fn run_battery_level_callback(id: &PeripheralId, battery_level: u8) {
    let cb = BATTERY_LEVEL_CALLBACK.read();
    if let Some(callback) = &*cb {
        callback(id.to_string(), battery_level);
    }
}
