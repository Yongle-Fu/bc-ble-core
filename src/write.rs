//! BLE write with MTU chunking.

use crate::adapter::get_central_adapter;
use crate::c_utils::to_peripheral_id;
use crate::connect::{find_peripheral, get_mtu};
use crate::runtime::spawn_any;
use btleplug::api::{CharPropFlags, Characteristic, Peripheral as _, WriteType};
use btleplug::platform::{Adapter, PeripheralId};
use uuid::Uuid;

fn select_write_characteristic<'a>(
    characteristics: impl Iterator<Item = &'a Characteristic> + Clone,
    service_uuids: &[Uuid],
    without_response: bool,
) -> Option<(&'a Characteristic, WriteType)> {
    let requested = if without_response {
        (
            CharPropFlags::WRITE_WITHOUT_RESPONSE,
            WriteType::WithoutResponse,
        )
    } else {
        (CharPropFlags::WRITE, WriteType::WithResponse)
    };
    let fallback = if without_response {
        (CharPropFlags::WRITE, WriteType::WithResponse)
    } else {
        (
            CharPropFlags::WRITE_WITHOUT_RESPONSE,
            WriteType::WithoutResponse,
        )
    };

    [requested, fallback]
        .into_iter()
        .find_map(|(property, write_type)| {
            characteristics
                .clone()
                .find(|characteristic| {
                    service_uuids.contains(&characteristic.service_uuid)
                        && characteristic.properties.contains(property)
                })
                .map(|characteristic| (characteristic, write_type))
        })
}

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
    let (tx_char, write_type) =
        select_write_characteristic(chars.iter(), service_uuids, without_response).ok_or_else(
            || anyhow::anyhow!("Write characteristic not found for peripheral: {id:?}"),
        )?;

    if without_response != (write_type == WriteType::WithoutResponse) {
        log::debug!(
            "Requested BLE write mode is unavailable; using {write_type:?} for characteristic {}",
            tx_char.uuid
        );
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn characteristic(service_uuid: Uuid, properties: CharPropFlags) -> Characteristic {
        Characteristic {
            uuid: uuid(2),
            service_uuid,
            properties,
            descriptors: BTreeSet::new(),
        }
    }

    #[test]
    fn follows_requested_mode_when_both_are_supported() {
        let service_uuid = uuid(1);
        let characteristic = characteristic(
            service_uuid,
            CharPropFlags::WRITE | CharPropFlags::WRITE_WITHOUT_RESPONSE,
        );

        let (_, write_type) =
            select_write_characteristic(std::iter::once(&characteristic), &[service_uuid], true)
                .unwrap();

        assert_eq!(write_type, WriteType::WithoutResponse);
    }

    #[test]
    fn falls_back_to_the_supported_write_mode() {
        let service_uuid = uuid(1);
        let characteristic = characteristic(service_uuid, CharPropFlags::WRITE);

        let (_, write_type) =
            select_write_characteristic(std::iter::once(&characteristic), &[service_uuid], true)
                .unwrap();

        assert_eq!(write_type, WriteType::WithResponse);
    }

    #[test]
    fn ignores_characteristics_from_other_services() {
        let service_uuid = uuid(1);
        let characteristic = characteristic(uuid(3), CharPropFlags::WRITE);

        assert!(
            select_write_characteristic(std::iter::once(&characteristic), &[service_uuid], false,)
                .is_none()
        );
    }
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
