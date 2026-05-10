//! BLE central adapter initialization and event listening.

use crate::callbacks::*;
use crate::runtime::spawn_any;
use crate::scan::is_scanning;
use crate::types::ScanResult;
use btleplug::api::Peripheral as _;
use btleplug::api::bleuuid::BleUuid;
use btleplug::api::{Central, CentralEvent, Manager as _};
use btleplug::platform::Adapter;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static::lazy_static! {
  pub static ref GLOBAL_CENTRAL: Arc<RwLock<Option<Adapter>>> = Arc::new(RwLock::new(None));
}

/// Get the global central adapter (None if not initialized).
pub fn get_central_adapter() -> Option<Adapter> {
    GLOBAL_CENTRAL.read().clone()
}

/// Initialize the BLE central adapter and start the event listener.
///
/// After initialization, call `set_received_data_callback` to wire up
/// your device's data parser. This must be done by the downstream SDK.
pub async fn initialize_central_adapter() -> Result<(), Box<dyn std::error::Error>> {
    log::debug!("Initializing central adapter...");
    let manager = btleplug::platform::Manager::new().await?;
    log::debug!("Bluetooth manager: {manager:?}");

    let adapters = manager.adapters().await?;
    log::debug!("Bluetooth adapters: {adapters:?}");

    if adapters.is_empty() {
        return Err("No Bluetooth adapters found.".into());
    }

    let central = adapters[0].clone();
    log::info!("Using adapter: {:?}", central.adapter_info().await?);

    for adapter in &adapters[1..] {
        log::debug!("Other adapter: {:?}", adapter.adapter_info().await?);
    }

    let mut global_central = GLOBAL_CENTRAL.write();
    *global_central = Some(central.clone());
    log::info!("Central adapter initialized.");

    spawn_any(async move {
        log::info!("Starting central event listener...");
        if let Err(e) = initialize_central_event_listener(central).await {
            log::warn!("Failed to initialize event listener: {e:?}");
        }
    });

    Ok(())
}

/// Central event listener loop.
///
/// Handles: StateUpdate, DeviceDiscovered/Updated (with Windows UUID filter),
/// DeviceConnected, DeviceDisconnected, ManufacturerDataAdvertisement, etc.
pub async fn initialize_central_event_listener(
    central: Adapter,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Initializing central event listener...");
    let mut events = central.events().await?;
    log::info!("central.events listener started.");

    while let Some(event) = events.next().await {
        match event {
            CentralEvent::StateUpdate(state) => {
                log::info!("Central StateUpdate: {state:?}");
                run_adapter_state_callback(state);
            }
            CentralEvent::DeviceDiscovered(id) | CentralEvent::DeviceUpdated(id) => {
                log::trace!("DeviceDiscovered or DeviceUpdated: {id:?}");
                if !is_registered_device_discovered() || !is_scanning() {
                    continue;
                }
                if let Ok(peripheral) = central.peripheral(&id).await
                    && let Ok(properties) = peripheral.properties().await
                {
                    let properties = properties.unwrap();

                    #[cfg(target_os = "windows")]
                    {
                        let filter_uuids = SCAN_FILTER_UUIDS.read().clone();
                        if !filter_uuids.is_empty() {
                            let device_addr = id.to_string();
                            let has_uuid =
                                filter_uuids.iter().any(|u| properties.services.contains(u));
                            let mut matched = MATCHED_DEVICES.write();
                            if has_uuid {
                                matched.insert(device_addr);
                            } else if !matched.contains(&device_addr) {
                                continue;
                            }
                        }
                    }

                    let device_address = id.to_string();
                    let device_name = properties.local_name.unwrap_or("N/A".to_string());
                    let rssi = properties.rssi.unwrap_or(0);

                    let service_uuids: Vec<String> = properties
                        .services
                        .iter()
                        .map(|uuid| uuid.to_string())
                        .collect();

                    let scan_result = ScanResult {
                        id: device_address,
                        name: device_name,
                        rssi,
                        service_uuids,
                    };

                    run_device_discovered_callback(scan_result);
                }
            }
            CentralEvent::DeviceConnected(id) => {
                log::info!("DeviceConnected: {id:?}");
            }
            CentralEvent::DeviceDisconnected(id) => {
                log::info!("DeviceDisconnected: {id:?}");
                run_connection_state_callback(&id, crate::types::ConnectionState::Disconnected);
            }
            CentralEvent::ManufacturerDataAdvertisement {
                id,
                manufacturer_data,
            } => {
                log::debug!("ManufacturerDataAdvertisement: {id:?}, {manufacturer_data:?}");
                if !is_registered_scan_result() || !is_scanning() {
                    continue;
                }
                let _ = handle_manufacturer_data(&central, &id, manufacturer_data).await;
            }
            CentralEvent::ServiceDataAdvertisement { id, service_data } => {
                log::trace!("ServiceDataAdvertisement: {id:?}, {service_data:?}");
            }
            CentralEvent::ServicesAdvertisement { id, services } => {
                let services: Vec<String> =
                    services.into_iter().map(|s| s.to_short_string()).collect();
                log::trace!("ServicesAdvertisement: {id:?}, {services:?}");
            }
            // Catch-all for newer CentralEvent variants (e.g. DeviceServicesModified, RssiUpdate)
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    log::info!("central.events stopped.");
    Ok(())
}

async fn handle_manufacturer_data(
    central: &Adapter,
    id: &btleplug::platform::PeripheralId,
    manufacturer_data: HashMap<u16, Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::trace!("ManufacturerDataAdvertisement: {id:?}, {manufacturer_data:?}");

    let p = central.peripheral(id).await?;
    if let Some(properties) = btleplug::api::Peripheral::properties(&p).await? {
        let device_address = id.to_string();
        let device_name = properties.local_name;
        let rssi = properties.rssi.unwrap_or(0);

        let service_uuids: Vec<String> = properties
            .services
            .iter()
            .map(|uuid| uuid.to_string())
            .collect();

        log::debug!(
            "Device address: {device_address:?}, name: {device_name:?}, RSSI: {rssi:?}, services: {service_uuids:?}"
        );
        let scan_result = ScanResult {
            id: device_address,
            name: device_name.unwrap_or("N/A".to_string()),
            rssi,
            service_uuids,
        };
        run_scan_result_callback(scan_result);
    }

    Ok(())
}

/// Initialize the BLE adapter synchronously (convenience wrapper).
pub fn ble_init_adapter() -> Result<(), anyhow::Error> {
    log::info!("ble_init_adapter");
    if get_central_adapter().is_none() {
        crate::runtime::block_on_any(async {
            let _ = initialize_central_adapter().await;
            log::info!("ble_init_adapter done.");
        });
    }
    Ok(())
}
