# RIACC: A Gateway-Assisted Adaptive Communication Management Framework for LoRa Networks

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Paper Status](https://img.shields.io/badge/Paper-Under%20Review%20(Elsevier%20Ad%20Hoc%20Networks)-orange.svg)](https://www.sciencedirect.com/journal/ad-hoc-networks)
[![Benchmark Scale](https://img.shields.io/badge/Trace%20Replay-2.0M%20Events%20%7C%203%2C541%20Nodes-green.svg)](#-dataset-provenance--included-benchmarks)

---

## 📌 Overview

This repository contains the official reference implementation, discrete-event simulation engine, trace replay pipeline, and publication figure generator for **RIACC (Runtime Intelligence and Adaptive Control-based Communication)**.

**RIACC** is a gateway-assisted medium access coordination framework designed for dense Long Range (LoRa / LPWAN) sensor deployments. Standard random access schemes (such as Pure ALOHA and uncoordinated LoRaWAN Class A) suffer severe co-channel contention and message dropouts during correlated traffic bursts (e.g., equipment failures, industrial gas leaks, physical security breaches, or DDoS-scale anomaly storms). 

RIACC resolves this challenge without requiring dedicated hardware time-synchronization (such as GPS/TSCH) or heavy neural network inference on edge microcontrollers:
1. **Edge-Side Urgency Formulation:** Distributed sensor nodes autonomously evaluate an **Adaptive Threat Index ($\text{ATI}$)** and burst acceleration score in $\mathcal{O}(1)$ time to manage local buffer priorities using a binary min-heap.
2. **Centralized Runtime Intelligence:** The master gateway maintains **7 in-memory operational state tables** to schedule conflict-free Time-on-Air (ToA) transmission slots and enforce closed-loop spectrum control directives ($\textbf{GRANT}$, $\textbf{HOLD}$, $\textbf{WAIT}$, $\textbf{RETRY}$, $\textbf{DROP}$).
3. **Emergency Preemption Fast-Path:** Critical hazard packets ($\text{ATI} \ge 60$) bypass standard request-grant negotiation and transmit immediately on a dedicated preemption sub-band ($865.9\text{ MHz}$, SF7), ensuring sub-second alarm delivery.

---

## 📦 Dataset Provenance & Included Benchmarks

To ensure **immediate out-of-the-box reproducibility**, the exact 2,000,000-event benchmark evaluation trace is **already included directly in the `dataset/` folder**:

* **Origin:** Extracted from the official **UNSW Canberra Cyber ToN_IoT Repository** (`ToN_IoT_datasets/Processed_datasets/Processed_Network_dataset/`).
* **Included Trace Files:**
  * [`dataset/Network_dataset_1.csv.gz`](dataset/Network_dataset_1.csv.gz) (`8.58 MB` compressed $\to$ 1,000,000 events): Quiescent telemetry & localized scanning surges.
  * [`dataset/Network_dataset_17.csv.gz`](dataset/Network_dataset_17.csv.gz) (`8.90 MB` compressed $\to$ 1,000,000 events): High-density DDoS and synchronized flooding traffic.
* **Automatic Decompression:** The simulation engine automatically streams and decompresses these files in memory—no manual extraction required.

---

## 📊 Comprehensive Experimental Results

Empirical results obtained from replaying **2,000,000 real network events** across **3,541 unique physical device identifiers** centered in a $R = 2000\text{ m}$ circular cell under ITU-R P.1411-10 suburban propagation:

| Performance Metric | Pure ALOHA<br>*(Baseline 1)* | Pure ALOHA + RIACC<br>*(Mode 2)* | LoRaWAN Class A<br>*(Baseline 3)* | LoRaWAN Class A + RIACC<br>*(Mode 4 - Proposed)* |
| :--- | :---: | :---: | :---: | :---: |
| **Total Replayed Events ($P_{\text{TX}}^{\text{total}}$)** | $2,000,000$ | $2,000,000$ | $2,000,000$ | **$2,000,000$** |
| **Total Delivered Packets ($P_{\text{RX}}^{\text{total}}$)** | $72,096$ | $186,386$ | $132,315$ | **$760,884$** |
| **Overall Delivery Ratio ($\text{PDR}_{\text{overall}}$)** | $3.60\%$ | $9.32\%$ *(+5.72 pp)* | $6.62\%$ | **$38.04\%$ *(+31.42 pp)*** |
| **Capacity Improvement Factor ($I_{\text{PDR}}$)** | $1.00\times$ | $2.59\times$ | $1.00\times$ | **$5.75\times$** |
| **Emergency Packets Generated ($P_{\text{TX,emerg}}$)** | $966,289$ | $966,289$ | $966,289$ | **$966,289$** |
| **Emergency Packets Delivered ($P_{\text{RX,emerg}}$)** | $1,848$ | $116,591$ | $3,667$ | **$528,935$** |
| **Emergency Delivery Ratio ($\text{PDR}_{\text{critical}}$)** | $0.19\%$ | $12.07\%$ *(+11.88 pp)* | $0.38\%$ | **$54.74\%$ *(+54.36 pp)*** |
| **Emergency Delivery Gain ($G_{\text{emerg}}$)** | $1.00\times$ | $63.5\times$ | $1.00\times$ | **$144.0\times$** |
| **Total Co-Channel Collisions ($P_{\text{collided}}$)** | $1,927,904$ | $930,203$ | $1,867,685$ | **$768,060$** |
| **Channel Collision Rate ($\text{CR}$)** | $96.40\%$ | $46.51\%$ *(-49.89 pp)* | $93.38\%$ | **$38.40\%$ *(-54.98 pp)*** |
| **Collisions Prevented ($\Delta P_{\text{collided}}$)** | — | $997,701$ *($51.75\%$)* | — | **$1,099,625$ *($58.88\%$)* ** |
| **Transceiver Energy per Delivered Pkt ($\eta_E$)** | $171,161\text{ mJ}$ | $40,017\text{ mJ}$ | $27,170\text{ mJ}$ | **$5,991\text{ mJ}$ *(5.99 J)*** |
| **Relative Energy Reduction ($\Delta E_{\text{rel}}$)** | — | $-76.62\%$ | — | **$-77.95\%$** |
| **Aggregate Throughput ($S$)** | $0.039\text{ pkts/s}$ | $0.101\text{ pkts/s}$ | $0.072\text{ pkts/s}$ | **$0.414\text{ pkts/s}$ *(331.0 bps)*** |
| **Physical Channel Utilization** | $26.85\%$ | $2.78\%$ | $7.82\%$ | **$1.20\%$** |
| **Master Scheduler Efficiency** | $0.0\%$ | $54.24\%$ | $0.0\%$ | **$61.88\%$** |

---

## 📁 Repository Structure

```
.
├── code/
│   ├── main.py                      # Multi-core simulation runner & pipeline driver
│   ├── config.py                    # LoRa RF parameterization & frequency plan
│   ├── models.py                    # Data classes (NodeEvent, Decision, Packet)
│   ├── simulation_engine.py         # Discrete-event scheduler & execution loop
│   ├── intelligent_master_node.py   # Gateway coordinator & 7 runtime tables
│   ├── intelligent_sensor_node.py   # Sensor node FSM, min-heap & ATI logic
│   ├── lora_channel.py              # ITU-R P.1411 channel & capture effect
│   ├── feature_engineering.py       # Differential sensing & burst estimation
│   ├── performance_metrics.py       # 17 macro & QoS performance analyzers
│   ├── dataset_loader.py            # Streaming ToN_IoT trace loader
│   ├── graph_generator.py           # Publication-ready figure generator
│   ├── packet_builder.py            # LoRa frame serializer & header piggybacking
│   ├── statistics_manager.py        # Timeseries aggregator & logger
│   └── results_exporter.py          # JSON/CSV result formatting engine
├── dataset/
│   ├── Network_dataset_1.csv.gz     # Benchmark trace 1 (1.0M events, compressed)
│   ├── Network_dataset_17.csv.gz    # Benchmark trace 2 (1.0M events, compressed)
│   └── README.md                    # Detailed dataset provenance and mapping
├── main.tex                         # Complete Elsevier Ad Hoc Networks LaTeX manuscript
├── references.bib                   # BibTeX bibliography with full DOIs
├── README.md                        # Project documentation & setup instructions
└── requirements.txt                 # Python dependencies
```

---

## 🚀 Quick Start & Installation

### 1. Prerequisites
Ensure you have **Python 3.10+** installed:
```bash
python --version
```

### 2. Clone the Repository & Install Dependencies
```bash
git clone https://github.com/safeer21-bit/riacc-lora-framework.git
cd riacc-lora-framework
pip install -r requirements.txt
```

---

## 💻 Running the Simulation & Generating Results

### 1. Execute Full 2.0M-Event 4-Scenario Evaluation:
```bash
python code/main.py
```
*Processes `Network_dataset_1.csv.gz` and `Network_dataset_17.csv.gz` across all 4 modes in parallel, automatically writing all summary tables, CSVs, and JSON logs into `results/files/`.*

### 2. Generate All 10 Publication Figures:
```bash
python code/graph_generator.py
```
*Generates all 10 high-resolution evaluation figures in vector `.pdf` and 300 DPI `.png` into `results/graphs/`.*

---

## 📄 Citation

```bibtex
@article{shah2026riacc,
  title={{RIACC}: A Gateway-Assisted Adaptive Communication Management Framework for {LoRa} Networks},
  author={Shah, Safeer Ahmad},
  journal={Ad Hoc Networks},
  year={2026},
  publisher={Elsevier},
  note={Under Review}
}
```

---

## 📜 License
This project is licensed under the MIT License.
