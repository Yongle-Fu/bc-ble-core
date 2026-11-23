//! BLE scan start/stop with Windows software UUID filter.

use crate::adapter::get_central_adapter;
use crate::callbacks::{clear_device_discovered_callback, clear_scan_result_callback};
use crate::runtime::block_on_any;
use btleplug::api::{Central, GattTransport, Peripheral as _, PeripheralProperties, ScanFilter};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

lazy_static::lazy_static! {
  pub(crate) static ref GLOBAL_SCANNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
  pub static ref SCAN_FILTER_UUIDS: Arc<RwLock<Vec<Uuid>>> = Arc::new(RwLock::new(Vec::new()));
  pub static ref MATCHED_DEVICES: Arc<RwLock<std::collections::HashSet<String>>> =
    Arc::new(RwLock::new(std::collections::HashSet::new()));
}

pub fn is_scanning() -> bool {
    GLOBAL_SCANNING.load(Ordering::Acquire)
}

pub(crate) fn set_scanning(scanning: bool) {
    GLOBAL_SCANNING.store(scanning, Ordering::Release);
}

pub async fn start_scan_with_uuids(service_uuids: Vec<Uuid>) -> Result<(), anyhow::Error> {
    start_scan_with_transport(service_uuids, GattTransport::Auto).await
}

pub async fn start_scan_with_transport(
    service_uuids: Vec<Uuid>,
    transport: GattTransport,
) -> Result<(), anyhow::Error> {
    log::info!("start_scan_with_uuids: {service_uuids:?}");
    if is_scanning() {
        return Err(anyhow::anyhow!("Scan already running."));
    }
    let central =
        get_central_adapter().ok_or_else(|| anyhow::anyhow!("No central adapter available"))?;
    let filter = ScanFilter {
        services: if cfg!(target_os = "windows") {
            Vec::new()
        } else {
            service_uuids.clone()
        },
    };
    begin_scan(service_uuids);
    if let Err(error) = central.start_scan_with_transport(filter, transport).await {
        set_scanning(false);
        return Err(error.into());
    }

    // BlueZ may emit only RSSI updates for devices already in its cache.
    // Initial adapter events can also predate the application's first scan.
    match central.peripherals().await {
        Ok(peripherals) => {
            for peripheral in peripherals {
                if let Ok(Some(properties)) = peripheral.properties().await {
                    deliver_discovered(&peripheral.id().to_string(), properties, None);
                }
            }
        }
        Err(error) => log::warn!("Could not enumerate cached scan devices: {error}"),
    }
    Ok(())
}

fn begin_scan(service_uuids: Vec<Uuid>) {
    *SCAN_FILTER_UUIDS.write() = service_uuids;
    MATCHED_DEVICES.write().clear();
    set_scanning(true);
}

pub(crate) fn deliver_discovered(id: &str, properties: PeripheralProperties, rssi: Option<i16>) {
    if !is_scanning() || !crate::callbacks::is_registered_device_discovered() {
        return;
    }
    let (has_filter, matches) = {
        let filters = SCAN_FILTER_UUIDS.read();
        (
            !filters.is_empty(),
            filters
                .iter()
                .any(|uuid| properties.services.contains(uuid)),
        )
    };
    #[cfg(target_os = "windows")]
    let matches = if has_filter {
        let mut matched = MATCHED_DEVICES.write();
        if matches {
            matched.insert(id.to_owned());
        }
        matches || matched.contains(id)
    } else {
        true
    };
    if has_filter && !matches {
        return;
    }
    crate::callbacks::run_device_discovered_callback(crate::types::ScanResult {
        id: id.to_owned(),
        name: properties.local_name.unwrap_or_else(|| "N/A".to_owned()),
        rssi: rssi.or(properties.rssi).unwrap_or(0),
        service_uuids: properties
            .services
            .iter()
            .map(ToString::to_string)
            .collect(),
    });
}

pub fn sync_start_scan_with_uuids(service_uuids: Vec<Uuid>) -> Result<(), anyhow::Error> {
    block_on_any(async { start_scan_with_uuids(service_uuids).await })
}

pub async fn stop_scan() -> Result<(), anyhow::Error> {
    log::info!("stop_scan");
    clear_scan_result_callback();
    clear_device_discovered_callback();
    if !is_scanning() {
        return Ok(());
    }
    let central = get_central_adapter().unwrap();
    let _ = central.stop_scan().await;
    set_scanning(false);
    Ok(())
}

pub fn sync_stop_scan() -> Result<(), anyhow::Error> {
    block_on_any(async { stop_scan().await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callbacks::*;
    use btleplug::api::CentralEvent;

    #[test]
    fn cached_devices_rssi_updates_filters_and_scan_restart_reach_callback() {
        // stop_scan clears both scan callbacks, which are process-global.
        let _scan_callbacks = crate::test_support::SCAN_CALLBACK_TEST_LOCK.lock();
        struct Cleanup;
        impl Drop for Cleanup {
            fn drop(&mut self) {
                set_scanning(false);
                SCAN_FILTER_UUIDS.write().clear();
                MATCHED_DEVICES.write().clear();
                clear_device_discovered_callback();
            }
        }
        let _cleanup = Cleanup;
        let results = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let captured = results.clone();
        set_device_discovered_callback(Box::new(move |result| captured.lock().push(result)));
        let service = Uuid::from_u128(7);
        let id = crate::test_support::device_id();
        let properties = || PeripheralProperties {
            local_name: Some("NexLoop-5556D".to_owned()),
            rssi: Some(-70),
            services: vec![service],
            ..Default::default()
        };

        begin_scan(vec![service]);
        // Use the same delivery function as the scan-start cached-device enumeration.
        deliver_discovered(&id.to_string(), properties(), None);
        {
            let results = results.lock();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].id, id.to_string());
            assert_eq!(results[0].name, "NexLoop-5556D");
            assert_eq!(results[0].rssi, -70);
            assert_eq!(results[0].service_uuids, vec![service.to_string()]);
        }
        for rssi in [-63, -60, -43] {
            let event = CentralEvent::RssiUpdate {
                id: id.clone(),
                rssi,
            };
            let (event_id, updated_rssi) = crate::adapter::discovery_update(&event).unwrap();
            deliver_discovered(&event_id.to_string(), properties(), updated_rssi);
            assert_eq!(results.lock().last().unwrap().rssi, rssi);
        }
        assert_eq!(results.lock().len(), 4);
        set_scanning(false);
        deliver_discovered(&id.to_string(), properties(), None);
        assert_eq!(results.lock().len(), 4);
        sync_stop_scan().unwrap();
        assert!(!is_registered_device_discovered());

        let captured = results.clone();
        set_device_discovered_callback(Box::new(move |result| captured.lock().push(result)));
        begin_scan(vec![Uuid::from_u128(8)]);
        deliver_discovered(&id.to_string(), properties(), None);
        assert_eq!(results.lock().len(), 4);
        set_scanning(false);
        begin_scan(vec![service]);
        deliver_discovered(&id.to_string(), properties(), None);
        assert_eq!(results.lock().len(), 5);

        for event in [
            CentralEvent::DeviceDiscovered(id.clone()),
            CentralEvent::DeviceUpdated(id.clone()),
            CentralEvent::ServicesAdvertisement {
                id: id.clone(),
                services: vec![service],
            },
            CentralEvent::ServiceDataAdvertisement {
                id: id.clone(),
                service_data: Default::default(),
            },
            CentralEvent::ManufacturerDataAdvertisement {
                id: id.clone(),
                manufacturer_data: Default::default(),
            },
        ] {
            assert!(crate::adapter::discovery_update(&event).is_some());
        }
        assert!(crate::adapter::discovery_update(&CentralEvent::DeviceDisconnected(id)).is_none());
    }
}
