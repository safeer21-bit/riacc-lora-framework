# ToN_IoT Network Telemetry Dataset

The full benchmark evaluation uses the standardized **ToN_IoT** telemetry dataset collected from an industrial IoT testbed.

- **Included Sample Trace:** `sample_network_trace.csv` (50,000 records) is provided for immediate out-of-the-box simulation testing.
- **Full Dataset Download (23 CSV files, ~3.3 GB):**
  The full dataset is publicly available from the UNSW Canberra Cyber Research Centre:
  👉 [UNSW ToN_IoT Dataset Portal](https://research.unsw.edu.au/projects/toniot-datasets)

### Setup Full Dataset:
1. Download the `Network_dataset_*.csv` files from the UNSW portal.
2. Place `Network_dataset_1.csv` through `Network_dataset_22.csv` into this `dataset/` directory.
3. Run `python code/main.py` to execute the full 2,000,000-event benchmark.
