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
pub fn try_to_peripheral_id(id: &str) -> anyhow::Result<PeripheralId> {
    match () {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        () => Ok(uuid::Uuid::parse_str(id)?.into()),

        #[cfg(target_os = "linux")]
        () => {
            let display = id.strip_prefix("/org/bluez/").unwrap_or(id);
            Ok(display.parse::<bluez_async::DeviceId>()?.into())
        }

        #[cfg(target_os = "windows")]
        () => Ok(BDAddr::from_str_delim(id)?.into()),

        #[cfg(target_os = "android")]
        () => {
            Ok(unsafe { std::mem::transmute::<BDAddr, PeripheralId>(BDAddr::from_str_delim(id)?) })
        }

        #[cfg(not(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "linux",
            target_os = "windows",
            target_os = "android"
        )))]
        () => anyhow::bail!("Unsupported operating system"),
    }
}

pub fn to_peripheral_id(id: &str) -> PeripheralId {
    try_to_peripheral_id(id).expect("Invalid BLE peripheral identifier")
}

/// Convert a C string device ID to a btleplug PeripheralId.
///
/// # Safety
/// `id` must point to a valid, null-terminated C string for this call.
pub unsafe fn to_peripheral_id_with_char(id: *const c_char) -> PeripheralId {
    let id_str = unsafe { CStr::from_ptr(id) };
    let id_str = id_str.to_str().expect("Invalid UTF-8 sequence");
    to_peripheral_id(id_str)
}

/// Convert a null-terminated C string array of UUID strings to a Vec<Uuid>.
///
/// # Safety
/// `services` must point to a readable, null-terminated array of valid C strings.
pub unsafe fn convert_to_uuids(
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
///
/// # Safety
/// A non-null `ptr` must point to a valid, null-terminated C string.
pub unsafe fn cstr_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let c_str = unsafe { CStr::from_ptr(ptr) };
    c_str.to_str().ok().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peripheral_id_round_trips_and_rejects_invalid_input() {
        let id = crate::test_support::device_id();
        assert_eq!(try_to_peripheral_id(&id.to_string()).unwrap(), id);
        assert!(try_to_peripheral_id("invalid peripheral id").is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bluez_peripheral_id_accepts_full_path_and_display_form() {
        let display = "hci0/dev_12_34_56_55_55_6D";
        let full_path = format!("/org/bluez/{display}");
        assert_eq!(
            try_to_peripheral_id(&full_path).unwrap(),
            try_to_peripheral_id(display).unwrap(),
        );
    }
}
