//! BLE callback storage and dispatch.
//!
//! This module provides the generic (Rust-side) callback infrastructure.
//! C FFI callback wrappers remain in each downstream SDK's `core_c.rs`.

use crate::types::*;
use btleplug::api::CentralState;
use btleplug::platform::PeripheralId;
use parking_lot::RwLock;
use std::sync::Arc;

// ==================== Callback type definitions ====================

pub type AdapterStateCallback = Box<dyn Fn(CentralState) + Send + Sync>;
pub type ScanResultCallback = Box<dyn Fn(ScanResult) + Send + Sync>;
pub type ConnectionStateCallback = Box<dyn Fn(String, ConnectionState) + Send + Sync>;
type ConnectionStateObserver = Arc<dyn Fn(String, ConnectionState) + Send + Sync>;
pub type BatteryLevelCallback = Box<dyn Fn(String, u8) + Send + Sync>;
pub type ReceivedDataCallback = Box<dyn Fn(String, Vec<u8>) + Send + Sync>;

// ==================== Global callback storage ====================

// Snapshot the registration before calling application code. A callback may replace
// itself, and a callback already in progress may finish after a replacement.
struct CallbackSlot<T: ?Sized>(RwLock<Option<Arc<T>>>);

impl<T: ?Sized> CallbackSlot<T> {
    const fn new() -> Self {
        Self(RwLock::new(None))
    }

    fn set(&self, callback: Option<Box<T>>) {
        let next = callback.map(Arc::from);
        let previous = std::mem::replace(&mut *self.0.write(), next);
        drop(previous);
    }

    fn snapshot(&self) -> Option<Arc<T>> {
        self.0.read().clone()
    }

    fn is_registered(&self) -> bool {
        self.0.read().is_some()
    }
}

static ADAPTER_STATE_CALLBACK: CallbackSlot<dyn Fn(CentralState) + Send + Sync> =
    CallbackSlot::new();
static DEVICE_DISCOVERED_CALLBACK: CallbackSlot<dyn Fn(ScanResult) + Send + Sync> =
    CallbackSlot::new();
static SCAN_RESULT_CALLBACK: CallbackSlot<dyn Fn(ScanResult) + Send + Sync> = CallbackSlot::new();
static CONNECTION_STATE_CALLBACK: CallbackSlot<dyn Fn(String, ConnectionState) + Send + Sync> =
    CallbackSlot::new();
static RECEIVED_DATA_CALLBACK: CallbackSlot<dyn Fn(String, Vec<u8>) + Send + Sync> =
    CallbackSlot::new();
static BATTERY_LEVEL_CALLBACK: CallbackSlot<dyn Fn(String, u8) + Send + Sync> = CallbackSlot::new();

static CONNECTION_STATE_OBSERVERS: RwLock<Vec<ConnectionStateObserver>> = RwLock::new(Vec::new());

// ==================== Adapter state ====================

pub fn set_adapter_state_callback(callback: AdapterStateCallback) {
    ADAPTER_STATE_CALLBACK.set(Some(callback));
}

pub fn run_adapter_state_callback(state: CentralState) {
    log::info!("run_adapter_state_callback: {state:?}");
    if let Some(callback) = ADAPTER_STATE_CALLBACK.snapshot() {
        callback(state);
    } else {
        log::error!("Adapter state callback is not set");
    }
}

// ==================== Device discovered (DeviceDiscovered + DeviceUpdated) ====================

pub fn set_device_discovered_callback(callback: ScanResultCallback) {
    DEVICE_DISCOVERED_CALLBACK.set(Some(callback));
}

pub fn clear_device_discovered_callback() {
    DEVICE_DISCOVERED_CALLBACK.set(None);
}

pub(crate) fn is_registered_device_discovered() -> bool {
    DEVICE_DISCOVERED_CALLBACK.is_registered()
}

pub fn run_device_discovered_callback(result: ScanResult) {
    if let Some(callback) = DEVICE_DISCOVERED_CALLBACK.snapshot() {
        callback(result);
    }
}

// ==================== Scan result (ManufacturerDataAdvertisement) ====================

pub fn set_scan_result_callback(callback: ScanResultCallback) {
    SCAN_RESULT_CALLBACK.set(Some(callback));
}

pub fn clear_scan_result_callback() {
    SCAN_RESULT_CALLBACK.set(None);
}

pub(crate) fn is_registered_scan_result() -> bool {
    SCAN_RESULT_CALLBACK.is_registered()
}

pub fn run_scan_result_callback(result: ScanResult) {
    if let Some(callback) = SCAN_RESULT_CALLBACK.snapshot() {
        callback(result);
    }
}

// ==================== Connection state ====================

pub fn set_connection_state_callback(callback: ConnectionStateCallback) {
    CONNECTION_STATE_CALLBACK.set(Some(callback));
}

/// Add an SDK lifecycle observer without replacing the application's callback.
pub fn add_connection_state_observer(callback: ConnectionStateCallback) {
    CONNECTION_STATE_OBSERVERS.write().push(Arc::from(callback));
}

pub fn run_connection_state_callback(id: &PeripheralId, state: ConnectionState) {
    log::debug!("run_connection_state_callback: {id}, {state:?}");
    let observers = CONNECTION_STATE_OBSERVERS.read().clone();
    for observer in observers {
        observer(id.to_string(), state);
    }
    if let Some(callback) = CONNECTION_STATE_CALLBACK.snapshot() {
        callback(id.to_string(), state);
    }
}

// ==================== Received data ====================

pub fn set_received_data_callback(callback: ReceivedDataCallback) {
    RECEIVED_DATA_CALLBACK.set(Some(callback));
}

pub fn run_received_data_callback(id: &PeripheralId, data: Vec<u8>) {
    if let Some(callback) = RECEIVED_DATA_CALLBACK.snapshot() {
        callback(id.to_string(), data);
    }
}

// ==================== Battery level ====================

pub fn set_battery_level_callback(callback: BatteryLevelCallback) {
    BATTERY_LEVEL_CALLBACK.set(Some(callback));
}

pub fn run_battery_level_callback(id: &PeripheralId, battery_level: u8) {
    if let Some(callback) = BATTERY_LEVEL_CALLBACK.snapshot() {
        callback(id.to_string(), battery_level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread, time::Duration};

    #[test]
    fn connection_callback_can_replace_itself() {
        let (sent, received) = mpsc::channel();
        set_connection_state_callback(Box::new(move |_, state| {
            set_connection_state_callback(Box::new(|_, _| {}));
            sent.send(state).unwrap();
        }));
        let dispatch = thread::spawn(|| {
            let id = crate::test_support::device_id();
            run_connection_state_callback(&id, ConnectionState::Connected);
            run_connection_state_callback(&id, ConnectionState::Disconnected);
        });
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            ConnectionState::Connected
        );
        dispatch.join().unwrap();
        assert!(received.try_recv().is_err());
        CONNECTION_STATE_CALLBACK.set(None);
    }

    #[test]
    fn scan_callback_can_replace_itself_and_its_successor_can_clear_itself() {
        let _scan_callbacks = crate::test_support::SCAN_CALLBACK_TEST_LOCK.lock();
        let (sent, received) = mpsc::channel();
        set_scan_result_callback(Box::new(move |_| {
            let sent = sent.clone();
            set_scan_result_callback(Box::new(move |event| {
                clear_scan_result_callback();
                sent.send(event.rssi).unwrap();
            }));
        }));
        let dispatch = thread::spawn(|| {
            run_scan_result_callback(ScanResult::default());
            run_scan_result_callback(ScanResult {
                rssi: -39,
                ..Default::default()
            });
        });
        assert_eq!(received.recv_timeout(Duration::from_secs(2)).unwrap(), -39);
        dispatch.join().unwrap();
        assert!(!is_registered_scan_result());
    }
}
