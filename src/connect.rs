//! BLE connect/disconnect and notification stream handling.

use crate::adapter::get_central_adapter;
use crate::callbacks::*;
use crate::constants::BATTERY_LEVEL_CHAR_UUID;
use crate::notifications::{
    NotificationStream, NotificationWorker, RECEIVE_QUEUE_CAPACITY, receive_notifications,
};
use crate::runtime::{spawn_any, spawn_any_with_handle, spawn_global_with_handle};
use crate::types::*;
use btleplug::api::{Central, CharPropFlags, Characteristic, GattTransport, Peripheral as _};
use btleplug::platform::{Adapter, Peripheral, PeripheralId};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
struct ConnectionSession {
    peripheral: Option<Peripheral>,
    notifications: Option<tokio::task::JoinHandle<()>>,
    notification_worker: Option<NotificationWorker>,
    epoch: u64,
    generation: u64,
}

#[derive(Default)]
struct ConnectionStateTracker {
    epoch: u64,
    active: bool,
    generation: u64,
    last_delivered: Option<(u64, ConnectionState)>,
    delivering: bool,
    pending: VecDeque<StateEvent>,
}

struct StateEvent {
    generation: u64,
    state: ConnectionState,
    epoch: Option<u64>,
    delivered: oneshot::Sender<bool>,
}

thread_local! {
  static IN_STATE_CALLBACK: Cell<bool> = const { Cell::new(false) };
}

/// Whether this thread is currently delivering a connection state callback.
pub fn in_connection_state_callback() -> bool {
    IN_STATE_CALLBACK.get()
}

impl ConnectionStateTracker {
    fn enqueue(&mut self, state: ConnectionState, epoch: Option<u64>) -> oneshot::Receiver<bool> {
        let (delivered, receiver) = oneshot::channel();
        self.pending.push_back(StateEvent {
            generation: self.generation,
            state,
            epoch,
            delivered,
        });
        receiver
    }
}

impl ConnectionSession {
    fn mark_inactive(&mut self, id: &PeripheralId) -> u64 {
        self.epoch = self.epoch.wrapping_add(1);
        self.peripheral = None;
        CONNECTED_PERIPHERALS.write().remove(id);
        let mut trackers = STATE_TRACKER.lock();
        let tracker = trackers.entry(id.clone()).or_default();
        tracker.epoch = self.epoch;
        tracker.active = false;
        self.epoch
    }

    fn mark_active(&mut self, id: &PeripheralId, peripheral: Peripheral) -> u64 {
        self.epoch = self.epoch.wrapping_add(1);
        CONNECTED_PERIPHERALS
            .write()
            .insert(id.clone(), peripheral.clone());
        self.peripheral = Some(peripheral);
        let mut trackers = STATE_TRACKER.lock();
        let tracker = trackers.entry(id.clone()).or_default();
        tracker.epoch = self.epoch;
        tracker.active = true;
        if tracker.last_delivered == Some((self.generation, ConnectionState::Disconnected)) {
            tracker.last_delivered = None;
        }
        self.epoch
    }

    async fn replace_notifications(
        &mut self,
        id: PeripheralId,
        rx: HashSet<(Uuid, Uuid)>,
        values: NotificationStream,
        disconnect_on_failure: Option<Peripheral>,
    ) {
        self.stop_notifications(&id).await;
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
                if let Some(peripheral) = disconnect_on_failure
                    && let Err(error) = peripheral.disconnect().await
                {
                    log::error!("Failed to disconnect after notification intake failure: {error}");
                }
            }
        }));
    }

    async fn stop_notifications(&mut self, id: &PeripheralId) {
        self.peripheral = None;
        CONNECTED_PERIPHERALS.write().remove(id);
        if let Some(task) = self.notifications.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(worker) = self.notification_worker.take() {
            worker.stop().await;
        }
    }
}

lazy_static::lazy_static! {
    static ref CONNECTIONS: parking_lot::Mutex<HashMap<PeripheralId, Arc<tokio::sync::Mutex<ConnectionSession>>>> =
        parking_lot::Mutex::new(HashMap::new());
    static ref CONNECTED_PERIPHERALS: parking_lot::RwLock<HashMap<PeripheralId, Peripheral>> =
        parking_lot::RwLock::new(HashMap::new());
    static ref STATE_TRACKER: parking_lot::Mutex<HashMap<PeripheralId, ConnectionStateTracker>> =
        parking_lot::Mutex::new(HashMap::new());
}

fn connection_session(id: &PeripheralId) -> Arc<tokio::sync::Mutex<ConnectionSession>> {
    CONNECTIONS.lock().entry(id.clone()).or_default().clone()
}

fn current_generation(id: &PeripheralId) -> u64 {
    STATE_TRACKER
        .lock()
        .get(id)
        .map_or(0, |tracker| tracker.generation)
}

fn begin_state(id: &PeripheralId, state: ConnectionState) -> (u64, oneshot::Receiver<bool>) {
    let mut trackers = STATE_TRACKER.lock();
    let tracker = trackers.entry(id.clone()).or_default();
    tracker.generation = tracker.generation.wrapping_add(1);
    (tracker.generation, tracker.enqueue(state, None))
}

fn enqueue_state(
    id: &PeripheralId,
    generation: u64,
    epoch: Option<u64>,
    state: ConnectionState,
) -> Option<oneshot::Receiver<bool>> {
    let mut trackers = STATE_TRACKER.lock();
    let tracker = trackers.entry(id.clone()).or_default();
    if tracker.generation != generation
        || epoch.is_some_and(|epoch| tracker.epoch != epoch)
        || (state == ConnectionState::Connected && !tracker.active)
    {
        return None;
    }
    Some(tracker.enqueue(state, epoch))
}

fn dispatch_states(id: &PeripheralId) {
    {
        let mut trackers = STATE_TRACKER.lock();
        let tracker = trackers.entry(id.clone()).or_default();
        if tracker.delivering {
            return;
        }
        tracker.delivering = true;
    }

    let id = id.clone();
    crate::runtime::get_runtime().spawn_blocking(move || {
        drain_states(&id, |state| {
            IN_STATE_CALLBACK.set(true);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_connection_state_callback(&id, state);
            }));
            IN_STATE_CALLBACK.set(false);
            if result.is_err() {
                log::error!("BLE connection state callback panicked: {id}");
            }
        });
    });
}

fn drain_states(id: &PeripheralId, mut notify: impl FnMut(ConnectionState)) {
    loop {
        let (event, current, should_notify) = {
            let mut trackers = STATE_TRACKER.lock();
            let tracker = trackers
                .get_mut(id)
                .expect("connection state tracker exists");
            let Some(event) = tracker.pending.pop_front() else {
                tracker.delivering = false;
                return;
            };
            let current = event.generation == tracker.generation
                && event.epoch.is_none_or(|epoch| tracker.epoch == epoch)
                && (event.state != ConnectionState::Connected || tracker.active);
            let key = (event.generation, event.state);
            let should_notify = current && tracker.last_delivered != Some(key);
            if should_notify {
                tracker.last_delivered = Some(key);
            }
            (event, current, should_notify)
        };
        if should_notify {
            // One dispatcher per device preserves order even if a callback enqueues a transition.
            notify(event.state);
        }
        let _ = event.delivered.send(current);
    }
}

async fn wait_for_state(
    id: &PeripheralId,
    generation: u64,
    delivered: oneshot::Receiver<bool>,
) -> anyhow::Result<()> {
    dispatch_states(id);
    anyhow::ensure!(
        delivered.await.unwrap_or(false) && current_generation(id) == generation,
        "BLE operation was superseded"
    );
    Ok(())
}

fn publish_disconnected(id: &PeripheralId, generation: u64, epoch: u64) {
    if enqueue_state(id, generation, Some(epoch), ConnectionState::Disconnected).is_some() {
        dispatch_states(id);
    }
}

/// Reuse the service handles discovered for the current connection without blocking on `ConnectionSession`.
pub(crate) async fn connected_peripheral(id: &PeripheralId) -> Option<Peripheral> {
    CONNECTED_PERIPHERALS.read().get(id).cloned()
}

/// Ignore a delayed disconnect event if a replacement connection is already live.
pub(crate) async fn handle_disconnected(central: &Adapter, id: &PeripheralId) {
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    let peripheral = match session.peripheral.clone() {
        Some(peripheral) => Some(peripheral),
        None => find_peripheral(central, id).await.ok(),
    };
    if let Some(peripheral) = peripheral
        && peripheral.is_connected().await.unwrap_or(false)
    {
        return;
    }
    session.stop_notifications(id).await;
    let epoch = session.mark_inactive(id);
    let generation = session.generation;
    drop(session);
    publish_disconnected(id, generation, epoch);
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

/// Resolve a known device for connecting, restoring its WinRT registry entry when missing.
pub async fn find_peripheral_for_connect(
    central: &Adapter,
    id: &PeripheralId,
) -> anyhow::Result<Peripheral> {
    let result = central.peripheral(id).await;
    #[cfg(target_os = "windows")]
    if matches!(result, Err(btleplug::Error::DeviceNotFound)) {
        return Ok(central.add_peripheral(id).await?);
    }
    Ok(result?)
}

/// Connect to a BLE peripheral (async).
///
/// The `service_uuids` parameter specifies which service UUIDs to look for
/// when discovering characteristics (device-specific).
pub async fn connect_ble_with_services(id: &str, service_uuids: Vec<Uuid>, pair: bool) {
    if let Some(central) = get_central_adapter() {
        let peripheral_id = crate::c_utils::to_peripheral_id(id);
        log::info!("connect_ble, peripheral_id: {peripheral_id:?}");

        if let Err(error) =
            perform_connect_with_state(central, peripheral_id, &service_uuids, pair, async {
                Ok(|| {})
            })
            .await
        {
            log::error!("Failed to connect to peripheral: {error:#}");
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

        if let Err(e) = perform_disconnect_with_state(central, peripheral_id).await {
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

pub async fn perform_disconnect(central: &Adapter, id: &PeripheralId) -> anyhow::Result<()> {
    disconnect_session(central, id, None).await
}

/// Disconnect and publish the state transitions for this operation.
pub fn perform_disconnect_with_state(
    central: Adapter,
    id: PeripheralId,
) -> impl Future<Output = anyhow::Result<()>> {
    let (generation, delivered) = begin_state(&id, ConnectionState::Disconnecting);
    async move {
        wait_for_state(&id, generation, delivered).await?;
        disconnect_session(&central, &id, Some(generation)).await?;
        let delivered = enqueue_state(&id, generation, None, ConnectionState::Disconnected)
            .ok_or_else(|| anyhow::anyhow!("BLE disconnect was superseded"))?;
        wait_for_state(&id, generation, delivered).await
    }
}

async fn disconnect_session(
    central: &Adapter,
    id: &PeripheralId,
    generation: Option<u64>,
) -> anyhow::Result<()> {
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    if let Some(generation) = generation {
        anyhow::ensure!(
            current_generation(id) == generation,
            "BLE disconnect was superseded"
        );
        session.generation = generation;
    }
    let peripheral = match session.peripheral.clone() {
        Some(peripheral) => Some(peripheral),
        None => find_peripheral(central, id).await.ok(),
    };
    if let Some(peripheral) = peripheral
        && let Err(error) = peripheral.disconnect().await
    {
        // A link lost before the explicit disconnect still needs local cleanup.
        if peripheral.is_connected().await.unwrap_or(false) {
            return Err(error.into());
        }
    }
    session.stop_notifications(id).await;
    let _ = session.mark_inactive(id);
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

struct PendingConnectState {
    id: PeripheralId,
    generation: u64,
    armed: bool,
}

impl Drop for PendingConnectState {
    fn drop(&mut self) {
        if self.armed
            && enqueue_state(
                &self.id,
                self.generation,
                None,
                ConnectionState::Disconnected,
            )
            .is_some()
        {
            dispatch_states(&self.id);
        }
    }
}

/// Connect after device-specific preparation and publish only this operation's state.
/// The action returned by preparation runs after setup and before `Connected` is delivered.
pub fn perform_connect_with_state<'a, F: FnOnce() + 'a>(
    central: Adapter,
    id: PeripheralId,
    service_uuids: &'a [Uuid],
    pair: bool,
    prepare: impl Future<Output = anyhow::Result<F>> + 'a,
) -> impl Future<Output = anyhow::Result<()>> + 'a {
    let (generation, delivered) = begin_state(&id, ConnectionState::Connecting);
    let mut pending = PendingConnectState {
        id: id.clone(),
        generation,
        armed: true,
    };
    async move {
        let pending = &mut pending;
        let result = async {
            wait_for_state(&id, generation, delivered).await?;
            let on_connected = prepare.await?;
            let (epoch, rollback) = connect_session(
                &central,
                &id,
                service_uuids,
                pair,
                GattTransport::Auto,
                Some(generation),
            )
            .await?;
            on_connected();
            let delivered = enqueue_state(&id, generation, Some(epoch), ConnectionState::Connected)
                .ok_or_else(|| anyhow::anyhow!("BLE link changed during connection setup"))?;
            wait_for_state(&id, generation, delivered).await?;
            anyhow::ensure!(
                STATE_TRACKER.lock().get(&id).is_some_and(|tracker| {
                    tracker.generation == generation && tracker.epoch == epoch && tracker.active
                }),
                "BLE link changed during connection setup"
            );
            if let Some(rollback) = rollback {
                rollback.commit();
            }
            Ok(())
        }
        .await;
        if result.is_err()
            && let Some(delivered) =
                enqueue_state(&id, generation, None, ConnectionState::Disconnected)
        {
            let _ = wait_for_state(&id, generation, delivered).await;
        }
        pending.armed = false;
        result
    }
}

struct ConnectCancelGuard {
    armed: bool,
    attempt_epoch: u64,
    generation: u64,
    id: PeripheralId,
    slot: Arc<tokio::sync::Mutex<ConnectionSession>>,
    peripheral: Option<Peripheral>,
}

impl ConnectCancelGuard {
    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for ConnectCancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let attempt_epoch = self.attempt_epoch;
        let generation = self.generation;
        let id = self.id.clone();
        let slot = self.slot.clone();
        let peripheral = self.peripheral.take();
        spawn_global_with_handle(async move {
            let mut session = slot.lock().await;
            if session.epoch != attempt_epoch || session.generation != generation {
                return;
            }
            if let Some(peripheral) = peripheral {
                let _ = peripheral.disconnect().await;
            }
            session.stop_notifications(&id).await;
            let epoch = session.mark_inactive(&id);
            let generation = session.generation;
            drop(session);
            publish_disconnected(&id, generation, epoch);
        });
    }
}

pub async fn perform_connect_with_transport(
    central: &Adapter,
    id: &PeripheralId,
    service_uuids: &[Uuid],
    pair: bool,
    transport: GattTransport,
) -> Result<(), anyhow::Error> {
    connect_session(central, id, service_uuids, pair, transport, None)
        .await
        .map(|(_, rollback)| {
            if let Some(rollback) = rollback {
                rollback.commit();
            }
        })
}

async fn connect_session(
    central: &Adapter,
    id: &PeripheralId,
    service_uuids: &[Uuid],
    pair: bool,
    transport: GattTransport,
    generation: Option<u64>,
) -> anyhow::Result<(u64, Option<ConnectCancelGuard>)> {
    let _ = pair;
    let slot = connection_session(id);
    let mut session = slot.lock().await;
    let current = current_generation(id);
    anyhow::ensure!(
        generation.is_none_or(|generation| generation == current),
        "BLE connect was superseded"
    );
    session.generation = current;
    if let Some(peripheral) = &session.peripheral
        && session
            .notifications
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        && peripheral.is_connected().await.unwrap_or(false)
    {
        return Ok((session.epoch, None));
    }
    session.stop_notifications(id).await;
    let attempt_epoch = session.mark_inactive(id);
    let peripheral = find_peripheral_for_connect(central, id).await?;

    let mut cancel_guard = ConnectCancelGuard {
        armed: true,
        attempt_epoch,
        generation: current,
        id: id.clone(),
        slot: slot.clone(),
        peripheral: Some(peripheral.clone()),
    };

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
        let battery_char = setup_data_stream(&peripheral, service_uuids).await?;
        Ok::<_, anyhow::Error>(battery_char)
    }
    .await;
    let battery_char = match prepared {
        Ok(battery_char) => battery_char,
        Err(error) => {
            let _ = peripheral.disconnect().await;
            session.stop_notifications(id).await;
            let _ = session.mark_inactive(id);
            cancel_guard.armed = false;
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
        session.stop_notifications(id).await;
        let _ = session.mark_inactive(id);
        cancel_guard.armed = false;
        anyhow::bail!("Notification intake failed during connection setup");
    }
    let epoch = session.mark_active(id, peripheral.clone());
    cancel_guard.attempt_epoch = epoch;
    drop(session);

    if let Some(battery_char) = battery_char {
        read_battery_level(&peripheral, &battery_char).await;
    }

    Ok((epoch, Some(cancel_guard)))
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

async fn setup_data_stream(
    p: &Peripheral,
    service_uuids: &[Uuid],
) -> Result<Option<Characteristic>, anyhow::Error> {
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

    let mut battery_char = None;
    if let Some(found) = chars.iter().find(|c| c.uuid == BATTERY_LEVEL_CHAR_UUID) {
        if let Err(e) = p.subscribe(found).await {
            log::error!("Failed to subscribe to battery: {e:?}");
        }
        battery_char = Some(found.clone());
    }

    Ok(battery_char)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::device_id;
    use btleplug::api::ValueNotification;
    use std::time::Duration;

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
            session.stop_notifications(&id).await;
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
        session.stop_notifications(&id).await;
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

    #[tokio::test]
    async fn delayed_states_cannot_overtake_replacement_connection() {
        let id = state_delivery_test_id();
        let (old, first) = begin_state(&id, ConnectionState::Connecting);
        let mut stale = None;
        let mut replacement = None;
        let mut seen = Vec::new();
        drain_states(&id, |state| {
            seen.push(state);
            if replacement.is_none() {
                stale = enqueue_state(&id, old, None, ConnectionState::Disconnected);
                let (generation, connecting) = begin_state(&id, ConnectionState::Connecting);
                assert!(enqueue_state(&id, old, None, ConnectionState::Disconnected).is_none());
                let mut trackers = STATE_TRACKER.lock();
                let tracker = trackers.get_mut(&id).unwrap();
                tracker.epoch = 7;
                tracker.active = true;
                drop(trackers);
                assert!(
                    enqueue_state(&id, generation, Some(6), ConnectionState::Connected).is_none()
                );
                let connected =
                    enqueue_state(&id, generation, Some(7), ConnectionState::Connected).unwrap();
                let duplicate =
                    enqueue_state(&id, generation, Some(7), ConnectionState::Connected).unwrap();
                replacement = Some((connecting, connected, duplicate));
            }
        });
        assert_eq!(
            seen,
            [
                ConnectionState::Connecting,
                ConnectionState::Connecting,
                ConnectionState::Connected
            ]
        );
        assert!(first.await.unwrap());
        assert!(!stale.unwrap().await.unwrap());
        let (connecting, connected, duplicate) = replacement.unwrap();
        assert!(connecting.await.unwrap());
        assert!(connected.await.unwrap());
        assert!(duplicate.await.unwrap());
    }

    fn state_delivery_test_id() -> PeripheralId {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let id = "00000000-0000-0000-0000-000000000002";
        #[cfg(target_os = "linux")]
        let id = "hci0/dev_12_34_56_55_55_6E";
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let id = "12:34:56:55:55:6E";
        crate::c_utils::to_peripheral_id(id)
    }

    #[tokio::test]
    async fn cancelled_connect_cleanup_does_not_tear_down_replacement_session() {
        let id = device_id();
        let slot = Arc::new(tokio::sync::Mutex::new(ConnectionSession::default()));
        let (source, _) = tokio::sync::broadcast::channel(16);
        let emg = Uuid::from_u128(1);

        // Old attempt records attempt_epoch = 1 and gets cancelled while holding slot.
        let attempt_epoch = {
            let mut session = slot.lock().await;
            let epoch = session.mark_inactive(&id);
            let guard = ConnectCancelGuard {
                armed: true,
                attempt_epoch: epoch,
                generation: session.generation,
                id: id.clone(),
                slot: slot.clone(),
                peripheral: None,
            };
            // Simulate immediate replacement attempt winning the session before cleanup runs:
            // replacement bumps epoch and installs notifications before slot is released.
            drop(guard);
            let replacement_epoch = session.mark_inactive(&id);
            assert_ne!(replacement_epoch, epoch);
            session
                .replace_notifications(
                    id.clone(),
                    HashSet::from([(Uuid::from_u128(7), emg)]),
                    notifications(source.subscribe()),
                    None,
                )
                .await;
            assert_eq!(source.receiver_count(), 1);
            replacement_epoch
        };

        // Give the spawned cleanup task from the cancelled old attempt time to run.
        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Replacement session must remain untouched with its epoch and notification stream intact.
        let mut session = slot.lock().await;
        assert_eq!(session.epoch, attempt_epoch);
        assert_eq!(source.receiver_count(), 1);
        session.stop_notifications(&id).await;
    }
}
