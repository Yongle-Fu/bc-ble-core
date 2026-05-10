//! BLE scan start/stop with Windows software UUID filter.

use crate::adapter::get_central_adapter;
use crate::callbacks::{clear_device_discovered_callback, clear_scan_result_callback};
use crate::runtime::block_on_any;
use btleplug::api::{Central, ScanFilter};
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
    log::info!("start_scan_with_uuids: {service_uuids:?}");
    if is_scanning() {
        return Err(anyhow::anyhow!("Scan already running."));
    }
    set_scanning(true);
    let central = get_central_adapter().unwrap();

    #[cfg(target_os = "windows")]
    {
        *SCAN_FILTER_UUIDS.write() = service_uuids;
        MATCHED_DEVICES.write().clear();
        let _ = central.start_scan(ScanFilter::default()).await;
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = central
            .start_scan(ScanFilter {
                services: service_uuids,
            })
            .await;
    }

    Ok(())
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
