//! BLE write with MTU chunking.

use crate::adapter::get_central_adapter;
use crate::c_utils::to_peripheral_id;
use crate::connect::{find_peripheral, get_mtu};
use crate::runtime::spawn_any;
use btleplug::api::{CharPropFlags, Peripheral as _, WriteType};
use btleplug::platform::{Adapter, PeripheralId};
use uuid::Uuid;

/// Write data to a BLE peripheral, splitting into MTU-sized chunks.
pub async fn perform_write_value(
    central: &Adapter,
    id: &PeripheralId,
    data: &[u8],
    without_response: bool,
    service_uuids: &[Uuid],
) -> Result<(), anyhow::Error> {
    log::trace!("Writing value to peripheral: {id:?}, data: {data:?}");
    let peripheral = find_peripheral(central, id).await?;

    #[cfg(target_os = "linux")]
    peripheral.discover_services().await?;

    let chars = peripheral.characteristics();
    let tx_char = chars
        .iter()
        .find(|c| {
            service_uuids.contains(&c.service_uuid)
                && (!without_response && c.properties.contains(CharPropFlags::WRITE)
                    || (without_response
                        && c.properties.contains(CharPropFlags::WRITE_WITHOUT_RESPONSE)))
        })
        .ok_or_else(|| anyhow::anyhow!("Write characteristic not found for peripheral: {id:?}"))?;

    let write_type = if without_response {
        WriteType::WithoutResponse
    } else {
        WriteType::WithResponse
    };

    // Split data into MTU-sized chunks
    let mtu = get_mtu() - 3; // 3 bytes for GATT header
    let mut data_len = data.len();
    let mut offset = 0;
    while data_len > 0 {
        let chunk_len = std::cmp::min(data_len, mtu);
        let chunk = &data[offset..offset + chunk_len];
        peripheral.write(tx_char, chunk, write_type).await?;
        data_len -= chunk_len;
        offset += chunk_len;
    }

    Ok(())
}

/// High-level async write helper.
pub async fn ble_write_value(
    id: &str,
    data: &[u8],
    without_response: bool,
    service_uuids: &[Uuid],
) {
    if let Some(central) = get_central_adapter() {
        let peripheral_id = to_peripheral_id(id);
        log::debug!(
            "write_value, peripheral_id: {:?}, data: {:02x?}",
            peripheral_id,
            data
        );
        if let Err(e) = perform_write_value(
            &central,
            &peripheral_id,
            data,
            without_response,
            service_uuids,
        )
        .await
        {
            log::error!("Write value process failed: {e:?}");
        }
        log::debug!("write_value done.");
    } else {
        log::error!("No central adapter available.");
    }
}

/// Synchronous write (offloaded to background task).
pub fn sync_write_value(
    id: &str,
    data: &[u8],
    without_response: bool,
    service_uuids: Vec<Uuid>,
) -> Result<(), anyhow::Error> {
    let id_clone = id.to_string();
    let data_clone = data.to_vec();
    spawn_any(async move {
        ble_write_value(&id_clone, &data_clone, without_response, &service_uuids).await;
    });
    Ok(())
}
