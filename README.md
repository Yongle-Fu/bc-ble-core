# bc-ble-core

Shared BLE core library for BrainCo Rust SDKs.

## Overview

This crate extracts the common BLE transport layer from:
- [`bc-band-sdk`](https://github.com/BrainCoTech/bc-band-sdk) — Myon EEG headband
- [`honda-sdk`](https://github.com/BrainCoTech/honda-sdk) — Honda EEG headband
- [`morpheus-sdk`](https://github.com/BrainCoTech/morpheus-sdk) — Morpheus/Melody sleep devices

## Architecture

```
bc-ble-core/src/
├── types.rs      # ScanResult, ConnectionState, BLEDeviceInfo, CentralAdapterState
├── runtime.rs    # Tokio runtime helpers (block_on_any, spawn_any)
├── constants.rs  # Standard BLE UUIDs (battery, device info)
├── callbacks.rs  # Callback storage and dispatch (Rust-side)
├── adapter.rs    # Central adapter init + event listener (Windows UUID filter)
├── scan.rs       # Scan start/stop with Windows software UUID filter
├── connect.rs    # Connect/disconnect, MTU, service discovery, notifications
├── write.rs      # BLE write with MTU chunking
└── c_utils.rs    # C FFI utilities (PeripheralId conversion, UUID parsing)
```

## Usage in downstream SDKs

Add as a git dependency:

```toml
[dependencies]
bc-ble-core = { git = "https://github.com/BrainCoTech/bc-ble-core", branch = "main" }
```

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
