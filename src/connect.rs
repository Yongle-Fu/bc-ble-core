//! BLE connect/disconnect and notification stream handling.

use crate::adapter::get_central_adapter;
use crate::callbacks::*;
use crate::constants::BATTERY_LEVEL_CHAR_UUID;
use crate::runtime::spawn_any;
use crate::types::*;
use btleplug::api::{Central, CharPropFlags, Characteristic, Peripheral as _};
use btleplug::platform::{Adapter, Peripheral, PeripheralId};
use futures::StreamExt;
use uuid::Uuid;

// MTU storage.
lazy_static::lazy_static! {
  pub(crate) static ref MTU: parking_lot::RwLock<usize> = parking_lot::RwLock::new(517);
}

pub fn set_mtu(mtu: usize) {
    if mtu < 23 {
        log::warn!("MTU value too small, ignoring.");
        return;
    }
    if mtu > 517 {
        log::warn!("MTU value too large, ignoring.");
        return;
    }
    *MTU.write() = mtu;
}

pub fn get_mtu() -> usize {
    *MTU.read()
}

pub async fn find_peripheral(
    central: &Adapter,
    id: &PeripheralId,
) -> Result<Peripheral, anyhow::Error> {
    central.peripheral(id).await.map_err(|e| anyhow::anyhow!(e))
}

/// Connect to a BLE peripheral (async).
///
/// The `service_uuids` parameter specifies which service UUIDs to look for
/// when discovering characteristics (device-specific).
pub async fn connect_ble_with_services(id: &str, service_uuids: Vec<Uuid>) {
    if let Some(central) = get_central_adapter() {
        let peripheral_id = crate::c_utils::to_peripheral_id(id);
        log::info!("connect_ble, peripheral_id: {peripheral_id:?}");

        run_connection_state_callback(&peripheral_id, ConnectionState::Connecting);
        match perform_connect(&central, &peripheral_id, &service_uuids).await {
            Ok(_) => {
                log::info!("Successfully connected to peripheral: {id:?}");
                run_connection_state_callback(&peripheral_id, ConnectionState::Connected);
            }
            Err(e) => {
                log::error!("Failed to connect to peripheral: {e:?}");
                run_connection_state_callback(&peripheral_id, ConnectionState::Disconnected);
            }
        }
    } else {
        log::error!("No central adapter available.");
    }
}

/// Disconnect from a BLE peripheral (async).
pub async fn disconnect_ble(id: &str) {
    if let Some(central) = get_central_adapter() {
        let peripheral_id = crate::c_utils::to_peripheral_id(id);
        log::info!("disconnect_ble, peripheral_id: {peripheral_id:?}");

        run_connection_state_callback(&peripheral_id, ConnectionState::Disconnecting);
        if let Err(e) = perform_disconnect(&central, &peripheral_id).await {
            log::error!("Failed to disconnect: {e:?}");
        }
    } else {
        log::error!("No central adapter available.");
    }
}

/// Synchronous connect (offloaded to background task).
pub fn sync_connect_ble(id: &str, service_uuids: Vec<Uuid>) -> Result<(), anyhow::Error> {
    let id_clone = id.to_string();
    spawn_any(async move {
        connect_ble_with_services(&id_clone, service_uuids).await;
    });
    Ok(())
}

/// Synchronous disconnect (offloaded to background task).
pub fn sync_disconnect_ble(id: &str) -> Result<(), anyhow::Error> {
    let id_clone = id.to_string();
    spawn_any(async move {
        disconnect_ble(&id_clone).await;
    });
    Ok(())
}

pub async fn perform_disconnect(central: &Adapter, id: &PeripheralId) -> Result<(), anyhow::Error> {
    let peripheral = find_peripheral(central, id).await?;
    peripheral.disconnect().await?;
    Ok(())
}

pub async fn perform_connect(
    central: &Adapter,
    id: &PeripheralId,
    service_uuids: &[Uuid],
) -> Result<(), anyhow::Error> {
    let peripheral = find_peripheral(central, id).await?;

    peripheral.connect().await?;

    log::info!("Discovering services...");
    peripheral.discover_services().await?;

    setup_data_stream(&peripheral, service_uuids).await?;

    // Request MTU on Android
    #[cfg(target_os = "android")]
    {
        let mtu = get_mtu();
        if mtu > 23 {
            peripheral.request_mtu(mtu).await?;
            log::info!("MTU requested: {:?}", mtu);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // Read BLE device info
    match read_ble_device_info(&peripheral).await {
        Ok(info) => log::info!("BLE Device info: {info:?}"),
        Err(e) => log::warn!("Failed to read BLE device info: {e:?}"),
    }

    let owned_uuids = service_uuids.to_vec();
    spawn_any(async move {
        if let Err(e) = process_notifications_stream(&peripheral, &owned_uuids).await {
            log::error!("Failed to process notifications: {e:?}");
        }
    });

    Ok(())
}

async fn read_characteristic(peripheral: &Peripheral, uuid: Uuid) -> Result<String, anyhow::Error> {
    if let Some(c) = peripheral.characteristics().iter().find(|c| c.uuid == uuid) {
        match peripheral.read(c).await {
            Ok(value) => Ok(String::from_utf8_lossy(&value).to_string()),
            Err(_) => Ok(String::new()),
        }
    } else {
        Ok(String::new())
    }
}

pub async fn read_ble_device_info(peripheral: &Peripheral) -> Result<BLEDeviceInfo, anyhow::Error> {
    use crate::constants::*;
    Ok(BLEDeviceInfo {
        manufacturer: read_characteristic(peripheral, MANUFACTURER_NAME_CHAR_UUID).await?,
        model: read_characteristic(peripheral, MODEL_NUMBER_CHAR_UUID).await?,
        serial: read_characteristic(peripheral, SERIAL_NUMBER_CHAR_UUID).await?,
        hardware: read_characteristic(peripheral, HARDWARE_REVISION_CHAR_UUID).await?,
        firmware: read_characteristic(peripheral, FIRMWARE_REVISION_CHAR_UUID).await?,
    })
}

async fn read_battery_level(peripheral: &Peripheral, battery_char: &Characteristic) {
    match peripheral.read(battery_char).await {
        Ok(value) => {
            let battery_level = value[0];
            log::info!("Read battery level: {battery_level:?}");
            if battery_level <= 100 {
                run_battery_level_callback(&peripheral.id(), battery_level);
            }
        }
        Err(e) => log::error!("Failed to read battery level: {e:?}"),
    }
}

async fn process_notifications_stream(
    peripheral: &Peripheral,
    rx_service_uuids: &[Uuid],
) -> Result<(), anyhow::Error> {
    let mut values = peripheral.notifications().await?;
    let id = peripheral.id();

    while let Some(value) = values.next().await {
        log::trace!("Received uuid: {:?}, value: {:?}", value.uuid, value.value);
        let data = value.value;

        // Check if notification comes from one of the device's RX characteristics
        let is_rx = peripheral.characteristics().iter().any(|c| {
            c.uuid == value.uuid
                && rx_service_uuids.contains(&c.service_uuid)
                && c.properties.contains(CharPropFlags::NOTIFY)
        });

        if is_rx {
            run_received_data_callback(&id, data);
        } else if value.uuid == BATTERY_LEVEL_CHAR_UUID {
            let battery_level = data[0];
            log::debug!("battery level: {battery_level:?}");
            run_battery_level_callback(&id, battery_level);
        }
    }
    Ok(())
}

async fn setup_data_stream(p: &Peripheral, service_uuids: &[Uuid]) -> Result<(), anyhow::Error> {
    let chars = p.characteristics();

    let rx_char = chars
        .iter()
        .find(|c| {
            service_uuids.contains(&c.service_uuid) && c.properties.contains(CharPropFlags::NOTIFY)
        })
        .ok_or_else(|| anyhow::anyhow!("Notify characteristic not found"))?;

    log::debug!("RX Characteristic: {rx_char:?}");
    p.subscribe(rx_char).await?;
    log::debug!("Subscribed to RX characteristic.");

    if let Some(battery_char) = chars.iter().find(|c| c.uuid == BATTERY_LEVEL_CHAR_UUID) {
        if let Err(e) = p.subscribe(battery_char).await {
            log::error!("Failed to subscribe to battery: {e:?}");
        }
        read_battery_level(p, battery_char).await;
    }

    Ok(())
}
