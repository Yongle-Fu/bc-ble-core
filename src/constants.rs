//! Standard BLE UUIDs shared across all devices.
//!
//! Device-specific service UUIDs (Myon, Honda, Morpheus, Melody) should be
//! defined in each downstream SDK's own `constants.rs`.

use btleplug::api::bleuuid::uuid_from_u16;
use uuid::Uuid;

// Battery Service
pub static BATTERY_SERVICE_UUID: Uuid = uuid_from_u16(0x180F);
pub static BATTERY_LEVEL_CHAR_UUID: Uuid = uuid_from_u16(0x2A19);

// Device Information Service
pub static DEVICE_INFO_SERVICE_UUID: Uuid = uuid_from_u16(0x180A);
pub static MANUFACTURER_NAME_CHAR_UUID: Uuid = uuid_from_u16(0x2A29);
pub static MODEL_NUMBER_CHAR_UUID: Uuid = uuid_from_u16(0x2A24);
pub static SERIAL_NUMBER_CHAR_UUID: Uuid = uuid_from_u16(0x2A25);
pub static HARDWARE_REVISION_CHAR_UUID: Uuid = uuid_from_u16(0x2A27);
pub static FIRMWARE_REVISION_CHAR_UUID: Uuid = uuid_from_u16(0x2A26);
