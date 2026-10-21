//! BLE connect/disconnect and notification stream handling.

use crate::adapter::get_central_adapter;
use crate::callbacks::*;
use crate::constants::BATTERY_LEVEL_CHAR_UUID;
use crate::notifications::{
    NotificationStream, NotificationWorker, RECEIVE_QUEUE_CAPACITY, receive_notifications,
};
use crate::runtime::{spawn_any, spawn_any_with_handle};
use crate::types::*;
use btleplug::api::{Central, CharPropFlags, Characteristic, GattTransport, Peripheral as _};
use btleplug::platform::{Adapter, Peripheral, PeripheralId};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use uuid::Uuid;

#[derive(Default)]
struct ConnectionSession {
    peripheral: Option<Peripheral>,
    notifications: Option<tokio::task::JoinHandle<()>>,
    notification_worker: Option<NotificationWorker>,
}

impl ConnectionSession {
    async fn replace_notifications(
        &mut self,
        id: PeripheralId,
        rx: HashSet<(Uuid, Uuid)>,
        values: NotificationStream,
        disconnect_on_failure: Option<Peripheral>,
    ) {
        self.stop_notifications().await;
        let callback_id = id.clone();
        let worker =
            NotificationWorker::start(id.to_string(), RECEIVE_QUEUE_CAPACITY, move |value| {
                deliver_notification(&callback_id, &rx, value);
            });
        let sender = worker.sender.clone();
        let cancelled = worker.cancelled.clone();
        self.notification_worker = Some(worker);
        self.notifications = Some(spawn_any_with_handle(async move {
            if let Err(reason) = receive_notifications(values, &sender, &cancelled).await {
                log::error!(
                    "Notification intake failed: device={id} reason={reason}; disconnecting"
                );
                if let Some(peripheral) = disconnect_on_failure {
                    if let Err(error) = peripheral.disconnect().await {
                        log::error!(
                            "Failed to disconnect after notification intake failure: {error}"
                        );
                    }
                }
            }
        }));
    }
    async fn stop_notifications(&mut self) {
        if let Some(task) = self.notifications.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(worker) = self.notification_worker.take() {
            worker.stop().await;
        }
        self.peripheral = None;
    }
}

lazy_static::lazy_static! {
    static ref CONNECTIONS: parking_lot::Mutex<HashMap<PeripheralId, Arc<tokio::sync::Mutex<ConnectionSession>>>> =
        parking_lot::Mutex::new(HashMap::new());
}

fn connection_session(id: &PeripheralId) -> Arc<tokio::sync::Mutex<ConnectionSession>> {
    CONNECTIONS.lock().entry(id.clone()).or_default().clone()
}

/// Reuse the service handles discovered for the current connection.
pub(crate) async fn connected_peripheral(id: &PeripheralId) -> Option<Peripheral> {
    connection_session(id).lock().await.peripheral.clone()
}

/// Ignore a delayed disconnect event if a replacement connection is already live.
pub(crate) async fn handle_disconnected(central: &Adapter, id: &PeripheralId) {
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    let peripheral = match session.peripheral.clone() {
        Some(peripheral) => Some(peripheral),
        None => find_peripheral(central, id).await.ok(),
    };
    if let Some(peripheral) = peripheral {
        if peripheral.is_connected().await.unwrap_or(false) {
            return;
        }
    }
    session.stop_notifications().await;
    drop(session);
    run_connection_state_callback(id, ConnectionState::Disconnected);
}

// MTU storage.
#[cfg(any(target_os = "macos", target_os = "ios"))]
lazy_static::lazy_static! {
  pub(crate) static ref MTU: parking_lot::RwLock<usize> = parking_lot::RwLock::new(247);
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
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
pub async fn connect_ble_with_services(id: &str, service_uuids: Vec<Uuid>, pair: bool) {
    if let Some(central) = get_central_adapter() {
        let peripheral_id = crate::c_utils::to_peripheral_id(id);
        log::info!("connect_ble, peripheral_id: {peripheral_id:?}");

        run_connection_state_callback(&peripheral_id, ConnectionState::Connecting);
        match perform_connect(&central, &peripheral_id, &service_uuids, pair).await {
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
pub fn sync_connect_ble(
    id: &str,
    service_uuids: Vec<Uuid>,
    pair: bool,
) -> Result<(), anyhow::Error> {
    let id_clone = id.to_string();
    spawn_any(async move {
        connect_ble_with_services(&id_clone, service_uuids, pair).await;
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
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    let peripheral = match session.peripheral.clone() {
        Some(peripheral) => peripheral,
        None => find_peripheral(central, id).await?,
    };
    if let Err(error) = peripheral.disconnect().await {
        // A link lost before the explicit disconnect still needs local cleanup.
        if peripheral.is_connected().await.unwrap_or(false) {
            return Err(error.into());
        }
    }
    session.stop_notifications().await;
    Ok(())
}

pub async fn perform_connect(
    central: &Adapter,
    id: &PeripheralId,
    service_uuids: &[Uuid],
    pair: bool,
) -> Result<(), anyhow::Error> {
    perform_connect_with_transport(central, id, service_uuids, pair, GattTransport::Auto).await
}

pub async fn perform_connect_with_transport(
    central: &Adapter,
    id: &PeripheralId,
    service_uuids: &[Uuid],
    pair: bool,
    transport: GattTransport,
) -> Result<(), anyhow::Error> {
    let _ = pair;
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    if let Some(peripheral) = &session.peripheral {
        if session
            .notifications
            .as_ref()
            .is_some_and(|task| !task.is_finished())
            && peripheral.is_connected().await.unwrap_or(false)
        {
            return Ok(());
        }
    }
    session.stop_notifications().await;
    let peripheral = find_peripheral(central, id).await?;

    peripheral.connect_with_transport(transport).await?;

    #[cfg(target_os = "windows")]
    if pair {
        log::info!("🔒 Initiating Win32 active BLE pairing (btleplug)...");
        if let Err(e) = peripheral.pair().await {
            log::warn!("⚠️ Win32 BLE pairing failed or already paired: {:?}", e);
        } else {
            log::info!("✓ Win32 BLE pairing successful");
        }
    }

    log::info!("Discovering services...");
    let prepared = async {
        peripheral.discover_services().await?;
        // Start consuming before subscriptions and ancillary reads can produce a
        // burst into the backend's small notification buffer.
        let values = peripheral.notifications().await?;
        let rx = peripheral
            .characteristics()
            .iter()
            .filter(|c| {
                service_uuids.contains(&c.service_uuid)
                    && c.properties.contains(CharPropFlags::NOTIFY)
            })
            .map(|c| (c.service_uuid, c.uuid))
            .collect();
        session
            .replace_notifications(peripheral.id(), rx, values, Some(peripheral.clone()))
            .await;
        setup_data_stream(&peripheral, service_uuids).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match prepared {
        Ok(()) => {}
        Err(error) => {
            let _ = peripheral.disconnect().await;
            session.stop_notifications().await;
            return Err(error);
        }
    };

    // Request MTU on Android
    #[cfg(target_os = "android")]
    {
        let mtu = get_mtu();
        if mtu > 23 {
            // Automatic on modern btleplug/Android
            log::info!("Explicit MTU requested: {:?}", mtu);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // Read BLE device info
    match read_ble_device_info(&peripheral).await {
        Ok(info) => log::info!("BLE Device info: {info:?}"),
        Err(e) => log::warn!("Failed to read BLE device info: {e:?}"),
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        log::info!(
            "macOS/iOS configured write fragmentation MTU: {} bytes (not a negotiated MTU measurement)",
            get_mtu()
        );
    }

    if session
        .notification_worker
        .as_ref()
        .is_some_and(|worker| worker.cancelled.load(std::sync::atomic::Ordering::Acquire))
    {
        let _ = peripheral.disconnect().await;
        session.stop_notifications().await;
        anyhow::bail!("Notification intake failed during connection setup");
    }
    session.peripheral = Some(peripheral);

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
            let Some(&battery_level) = value.first() else {
                return;
            };
            log::info!("Read battery level: {battery_level:?}");
            if battery_level <= 100 {
                run_battery_level_callback(&peripheral.id(), battery_level);
            }
        }
        Err(e) => log::error!("Failed to read battery level: {e:?}"),
    }
}

fn deliver_notification(
    id: &PeripheralId,
    rx_characteristics: &HashSet<(Uuid, Uuid)>,
    value: btleplug::api::ValueNotification,
) {
    log::trace!("Received uuid: {:?}, value: {:?}", value.uuid, value.value);
    let data = value.value;

    // Check if notification comes from one of the device's RX characteristics
    let is_rx = rx_characteristics.contains(&(value.service_uuid, value.uuid));

    if is_rx {
        run_received_data_callback(id, data);
    } else if value.uuid == BATTERY_LEVEL_CHAR_UUID {
        let Some(&battery_level) = data.first() else {
            return;
        };
        log::debug!("battery level: {battery_level:?}");
        run_battery_level_callback(id, battery_level);
    }
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use btleplug::api::ValueNotification;
    use std::time::Duration;

    pub(crate) fn device_id() -> PeripheralId {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let id = "00000000-0000-0000-0000-000000000001";
        #[cfg(target_os = "linux")]
        let id = "hci0/dev_12_34_56_55_55_6D";
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let id = "12:34:56:55:55:6D";
        crate::c_utils::to_peripheral_id(id)
    }

    fn notifications(
        receiver: tokio::sync::broadcast::Receiver<ValueNotification>,
    ) -> NotificationStream {
        Box::pin(futures::stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.ok().map(|value| (value, receiver)) },
        ))
    }

    #[tokio::test]
    async fn reconnect_delivers_emg_and_imu_once_and_drops_old_receivers() {
        let (received, mut delivered) = tokio::sync::mpsc::unbounded_channel();
        set_received_data_callback(Box::new(move |id, data| {
            received.send((id, data)).unwrap();
        }));
        let id = device_id();
        let emg = Uuid::from_u128(1);
        let imu = Uuid::from_u128(2);
        let (source, _) = tokio::sync::broadcast::channel(16);
        let mut session = ConnectionSession::default();

        for cycle in 0..3 {
            // Replies arriving before the processing task is scheduled must be retained.
            let stream = notifications(source.subscribe());
            for uuid in [emg, imu] {
                source
                    .send(ValueNotification {
                        uuid,
                        service_uuid: Uuid::from_u128(7),
                        value: vec![cycle, uuid.as_u128() as u8],
                    })
                    .unwrap();
            }
            session
                .replace_notifications(
                    id.clone(),
                    HashSet::from([(Uuid::from_u128(7), emg), (Uuid::from_u128(7), imu)]),
                    stream,
                    None,
                )
                .await;
            for expected in [vec![cycle, 1], vec![cycle, 2]] {
                let frame = tokio::time::timeout(Duration::from_secs(1), delivered.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(frame, (id.to_string(), expected));
            }
            assert_eq!(source.receiver_count(), 1);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), delivered.recv())
                    .await
                    .is_err()
            );
            session.stop_notifications().await;
            assert_eq!(source.receiver_count(), 0);
            assert!(
                source
                    .send(ValueNotification {
                        uuid: emg,
                        service_uuid: Uuid::from_u128(7),
                        value: vec![99]
                    })
                    .is_err()
            );
        }

        // Replacing an active listener also waits for its receiver to disappear.
        for _ in 0..3 {
            session
                .replace_notifications(
                    id.clone(),
                    HashSet::from([(Uuid::from_u128(7), emg)]),
                    notifications(source.subscribe()),
                    None,
                )
                .await;
            assert_eq!(source.receiver_count(), 1);
        }
        session.stop_notifications().await;
        assert_eq!(source.receiver_count(), 0);
    }

    #[tokio::test]
    async fn lifecycle_operations_share_the_same_device_lock() {
        let id = device_id();
        let first = connection_session(&id);
        let second = connection_session(&id);
        assert!(Arc::ptr_eq(&first, &second));
        let held = first.lock().await;
        assert!(second.try_lock().is_err());
        drop(held);
        assert!(second.try_lock().is_ok());
    }
}
