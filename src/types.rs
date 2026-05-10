//! Shared BLE data types.

/// BLE device information (from Device Information Service 0x180A).
#[derive(Debug, Clone, Default)]
pub struct BLEDeviceInfo {
    pub manufacturer: String,
    pub model: String,
    pub serial: String,
    pub hardware: String,
    pub firmware: String,
}

/// BLE scan result.
#[derive(Debug, Clone, Default)]
pub struct ScanResult {
    pub id: String,
    pub name: String,
    pub rssi: i16,
    /// Service UUIDs advertised by the device.
    pub service_uuids: Vec<String>,
}

/// BLE adapter state (mapped from btleplug CentralState).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CentralAdapterState {
    Unknown = 0,
    PoweredOn = 1,
    PoweredOff = 2,
}

impl Default for CentralAdapterState {
    fn default() -> Self {
        Self::Unknown
    }
}

impl From<u8> for CentralAdapterState {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::PoweredOn,
            2 => Self::PoweredOff,
            _ => Self::Unknown,
        }
    }
}

/// BLE connection state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting = 0,
    Connected = 1,
    Disconnecting = 2,
    Disconnected = 3,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::Disconnected
    }
}

impl From<u8> for ConnectionState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Connecting,
            1 => Self::Connected,
            2 => Self::Disconnecting,
            _ => Self::Disconnected,
        }
    }
}
