//! C FFI utility functions for BLE PeripheralId conversion and UUID parsing.

#[allow(unused_imports)]
use btleplug::api::BDAddr;
use btleplug::platform::PeripheralId;
use std::ffi::{CStr, c_char};
use uuid::Uuid;

/// Convert a Rust string device ID to a btleplug PeripheralId.
///
/// Platform-specific:
/// - macOS/iOS: UUID string
/// - Linux: bluez device path
/// - Windows/Android: MAC address (colon-delimited)
pub fn to_peripheral_id(id: &str) -> PeripheralId {
    match () {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        () => uuid::Uuid::parse_str(id).unwrap().into(),

        #[cfg(target_os = "linux")]
        () => {
            let id = format!("/org/bluez/{}", id);
            PeripheralId::from_str(&id)
        }

        #[cfg(target_os = "windows")]
        () => BDAddr::from_str_delim(id).unwrap().into(),

        #[cfg(target_os = "android")]
        () => unsafe { std::mem::transmute(BDAddr::from_str_delim(id).unwrap()) },

        #[cfg(not(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "linux",
            target_os = "windows",
            target_os = "android"
        )))]
        () => panic!("Unsupported operating system"),
    }
}

/// Convert a C string device ID to a btleplug PeripheralId.
pub fn to_peripheral_id_with_char(id: *const c_char) -> PeripheralId {
    let id_str = unsafe { CStr::from_ptr(id) };
    let id_str = id_str.to_str().expect("Invalid UTF-8 sequence");
    to_peripheral_id(id_str)
}

/// Convert a null-terminated C string array of UUID strings to a Vec<Uuid>.
pub fn convert_to_uuids(
    services: *const *const c_char,
) -> Result<Vec<Uuid>, Box<dyn std::error::Error>> {
    let mut uuids = Vec::new();
    let mut current = services;

    unsafe {
        while !(*current).is_null() {
            let c_str = CStr::from_ptr(*current);
            let str_slice = c_str.to_str()?;
            log::trace!("convert_to_uuids, str_slice: {str_slice:?}");

            match Uuid::parse_str(str_slice) {
                Ok(uuid) => uuids.push(uuid),
                Err(e) => {
                    log::warn!("Failed to parse UUID from string '{str_slice}': {e:?}");
                    return Err(Box::new(e));
                }
            }

            current = current.add(1);
        }
    }
    log::debug!("convert_to_uuids, uuids: {uuids:?}");
    Ok(uuids)
}

/// Convert a C string pointer to an owned Rust String.
pub fn cstr_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let c_str = unsafe { CStr::from_ptr(ptr) };
    c_str.to_str().ok().map(|s| s.to_string())
}
