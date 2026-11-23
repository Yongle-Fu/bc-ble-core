//! BLE central adapter initialization and event listening.

use crate::callbacks::*;
use crate::runtime::spawn_any;
use crate::scan::is_scanning;
use crate::types::ScanResult;
use btleplug::api::Peripheral as _;
use btleplug::api::{Central, CentralEvent, Manager as _};
use btleplug::platform::Adapter;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static::lazy_static! {
  static ref INITIALIZE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
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
    let _initializing = INITIALIZE_LOCK.lock().await;
    if get_central_adapter().is_some() {
        return Ok(());
    }
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

    let events = central.events().await?;
    *GLOBAL_CENTRAL.write() = Some(central.clone());
    log::info!("Central adapter initialized.");

    spawn_any(async move {
        log::info!("Starting central event listener...");
        if let Err(e) = listen_for_events(central, events).await {
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
    let events = central.events().await?;
    listen_for_events(central, events).await
}

async fn listen_for_events(
    central: Adapter,
    mut events: std::pin::Pin<Box<dyn futures::Stream<Item = CentralEvent> + Send>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("central.events listener started.");

    while let Some(event) = events.next().await {
        if let Some((id, rssi)) = discovery_update(&event) {
            deliver_device_update(&central, id, rssi).await;
        }
        match event {
            CentralEvent::StateUpdate(state) => {
                log::info!("Central StateUpdate: {state:?}");
                run_adapter_state_callback(state);
            }
            CentralEvent::DeviceConnected(id) => {
                log::info!("DeviceConnected: {id:?}");
            }
            CentralEvent::DeviceDisconnected(id) => {
                log::info!("DeviceDisconnected: {id:?}");
                let central = central.clone();
                crate::runtime::spawn_any(async move {
                    crate::connect::handle_disconnected(&central, &id).await;
                });
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
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    log::info!("central.events stopped.");
    Ok(())
}

pub(crate) fn discovery_update(
    event: &CentralEvent,
) -> Option<(&btleplug::platform::PeripheralId, Option<i16>)> {
    match event {
        CentralEvent::DeviceDiscovered(id)
        | CentralEvent::DeviceUpdated(id)
        | CentralEvent::ServicesAdvertisement { id, .. }
        | CentralEvent::ServiceDataAdvertisement { id, .. }
        | CentralEvent::ManufacturerDataAdvertisement { id, .. } => Some((id, None)),
        CentralEvent::RssiUpdate { id, rssi } => Some((id, Some(*rssi))),
        _ => None,
    }
}

async fn deliver_device_update(
    central: &Adapter,
    id: &btleplug::platform::PeripheralId,
    rssi: Option<i16>,
) {
    if !is_scanning() || !is_registered_device_discovered() {
        return;
    }
    if let Ok(peripheral) = central.peripheral(id).await
        && let Ok(Some(properties)) = peripheral.properties().await
    {
        crate::scan::deliver_discovered(&id.to_string(), properties, rssi);
    }
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
