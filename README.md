# bc-ble-core

Shared BLE core library for BrainCo Rust SDKs.

## Overview

`bc-ble-core` owns the adapter and scan flow, connection and notification
lifecycle, and per-device GATT writes. The `btleplug` dependency implements
the platform BLE backends.

Downstream SDKs such as [NexLoop](https://github.com/BrainCoTech/bc-band-sdk),
[Honda](https://github.com/BrainCoTech/honda-sdk), and
[Morpheus](https://github.com/BrainCoTech/morpheus-sdk) consume the shared crate
for their Rust BLE paths. Each SDK owns its device UUIDs, protocol, public APIs,
and language bindings. An application that handles BLE itself can use its SDK's
application-managed transport where the SDK provides one.

## Architecture

```
bc-ble-core/src/
├── types.rs      # ScanResult, ConnectionState, BLEDeviceInfo, CentralAdapterState
├── runtime.rs    # Tokio runtime helpers (block_on_any, spawn_any)
├── constants.rs  # Standard BLE UUIDs (battery, device info)
├── callbacks.rs  # Callback storage and dispatch (Rust-side)
├── adapter.rs    # Central adapter initialization and event listener
├── scan.rs       # Scan lifecycle, cached discovery, and service UUID filtering
├── connect.rs    # Connection and subscription lifecycle
├── notifications.rs # Receive queue and callback worker
├── write.rs      # Per-device frame serialization and MTU-sized writes
└── c_utils.rs    # C FFI utilities (PeripheralId conversion, UUID parsing)
```

## Usage in downstream SDKs

Pin the shared crate to a reviewed Git revision and keep the resolved revision
in the downstream lockfile.

```toml
[dependencies]
bc-ble-core = { git = "https://github.com/Yongle-Fu/bc-ble-core.git", rev = "b048a1fcaab5e195c6c0768f66aa27748397084e" }
```

If the SDK also declares `btleplug` directly, keep it on the same Git source and
revision as the shared crate.

Each SDK provides its own service UUIDs and wires up `received_data_callback`:

```rust
use bc_ble_core::prelude::*;

// Define device-specific constants
static MY_SERVICE_UUID: Uuid = Uuid::from_u128(0x4de5a20c_0001_ae20_bf63_0242ac130002);

// Initialize and wire up
bc_ble_core::adapter::ble_init_adapter()?;
bc_ble_core::callbacks::set_received_data_callback(Box::new(|id, data| {
    MyDevice::receive_data(id, data);
}));

// Scan
start_scan_with_uuids(vec![MY_SERVICE_UUID]).await?;

// Connect (service UUIDs determine which characteristics to subscribe to)
connect_ble_with_services("device-id", vec![MY_SERVICE_UUID]).await;
```

## License

MIT
