# Dataset Configuration & Provenance

## 📦 Included Benchmark Datasets (2,000,000 Total Events)

To ensure **100% immediate out-of-the-box reproducibility**, the exact trace datasets evaluated in the research paper are **already bundled directly in this directory** in gzip-compressed format (`.csv.gz`). The simulation engine (`code/main.py`) automatically decompresses and streams them in real time:

| Dataset File | Compressed Size | Uncompressed Records | Traffic Profile & Workload Characteristics |
| :--- | :---: | :---: | :--- |
| **`Network_dataset_1.csv.gz`** | `8.58 MB` | **1,000,000 events** | Quiescent baseline telemetry mixed with localized scanning and port probing (Low-to-Moderate Contention, $\text{ATI} < 25$). |
| **`Network_dataset_17.csv.gz`** | `8.90 MB` | **1,000,000 events** | High-density synchronized packet flooding and distributed denial-of-service surges (Severe Contention Collapse, $\text{ATI} \ge 30$). |
| **Combined Evaluation Trace** | **`17.48 MB`** | **2,000,000 events** | **Full paper benchmark replayed across 3,541 unique physical device identifiers.** |

---

## 🏛️ Dataset Origin & Exact Folder Hierarchy

The evaluation traces are derived from the standardized **ToN_IoT** cybersecurity and industrial IoT telemetry benchmark published by the **UNSW Canberra Cyber Research Centre**:

* **Official Portal:** [UNSW Canberra ToN_IoT Datasets](https://research.unsw.edu.au/projects/toniot-datasets)
* **Exact Archive & Folder Path:**
  ```text
  Google Drive / UNSW Storage
  └── Processed_datasets/
      └── Processed_Network_dataset/
          ├── Network_dataset_1.csv   (1,000,000 records) -> Bundled as Network_dataset_1.csv.gz
          └── Network_dataset_17.csv  (1,000,000 records) -> Bundled as Network_dataset_17.csv.gz
  ```

---

## 🔬 Physical Parameter Mapping

In this research, the cybersecurity attack traffic is stripped of IP-level semantics and repurposed as a deterministic physical-layer traffic generator to model large-scale, synchronized hazard events (such as industrial gas leaks, chemical spills, or seismic triggers):

1. **`src_ip` $\to$ Physical Node ID:** Each of the 3,541 unique IP addresses is mapped to an autonomous LoRa sensor node distributed across a $R = 2000\text{ m}$ circular cell.
2. **`ts` (Timestamp) $\to$ Event Arrival Time ($t_{i,m}^{\text{arr}}$):** Dictates the exact microsecond arrival of sensor data into the node's local queue.
3. **`type` / Anomaly Severity $\to$ Edge Urgency Proxy:** Mapped via deterministic proxy to discrete Adaptive Threat Index ($\text{ATI}$) levels ($20, 35, 50, 75, 100$).
4. **`src_bytes` $\to$ Payload Size ($\text{PL}$):** Standardized to 32-byte frames ($T_{\text{air}} = 61.7\text{ ms}$ at SF7) to isolate MAC-layer contention dynamics from packet length variation.

---

## 🚀 How to Run

No manual dataset downloads or extraction steps are needed. The simulation engine will automatically stream datasets 1 & 17 and generate all output results, CSVs, and figures:

```bash
# Replays the complete 2,000,000-event trace across all 4 operational modes
python code/main.py

# Generates all 10 high-resolution publication figures
python code/graph_generator.py
```
