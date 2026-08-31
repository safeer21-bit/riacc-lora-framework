# RIACC: A Gateway-Assisted Adaptive Communication Management Framework for LoRa Networks

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Paper Status](https://img.shields.io/badge/Paper-Under%20Review%20(Elsevier%20Ad%20Hoc%20Networks)-orange.svg)](https://www.sciencedirect.com/journal/ad-hoc-networks)
[![Benchmark Scale](https://img.shields.io/badge/Trace%20Replay-2.0M%20Events%20%7C%203%2C541%20Nodes-green.svg)](#experimental-workload--trace-replay)

---

## Overview

This repository contains the official reference implementation, discrete-event simulation engine, trace replay pipeline, and figure generation suite for **RIACC (Runtime Intelligence and Adaptive Control-based Communication)**.

**RIACC** is a gateway-assisted medium access coordination framework designed for dense Long Range (LoRa / LPWAN) sensor deployments. Standard random access schemes (such as Pure ALOHA and uncoordinated LoRaWAN Class A) suffer severe co-channel contention and message dropouts during correlated traffic bursts (e.g., equipment failures, industrial gas leaks, physical security breaches, or DDoS-scale anomaly storms). 

RIACC resolves this challenge without requiring dedicated hardware time-synchronization (such as GPS/TSCH) or heavy neural network inference on edge microcontrollers:
1. **Edge-Side Urgency Formulation:** Distributed sensor nodes autonomously evaluate an **Adaptive Threat Index (ATI)** and burst acceleration score in O(1) time to manage local buffer priorities using a binary min-heap.
2. **Centralized Runtime Intelligence:** The master gateway maintains **7 in-memory operational state tables** to schedule conflict-free Time-on-Air (ToA) transmission slots and enforce closed-loop spectrum control directives (GRANT, HOLD, WAIT, RETRY, DROP).
3. **Emergency Preemption Fast-Path:** Critical hazard packets (ATI >= 60) bypass standard request-grant negotiation and transmit immediately on a dedicated preemption sub-band (865.9 MHz, SF7), ensuring sub-second alarm delivery.

---

## Comprehensive Experimental Results

Empirical results obtained from replaying **2,000,000 real network events** from **3,541 unique physical device identifiers** (derived from the standardized ToN_IoT benchmark) centered across a R = 2000 m circular cell under ITU-R P.1411-10 suburban propagation:

| Performance Metric | Pure ALOHA<br>*(Baseline 1)* | Pure ALOHA + RIACC<br>*(Mode 2)* | LoRaWAN Class A<br>*(Baseline 3)* | LoRaWAN Class A + RIACC<br>*(Mode 4 - Proposed)* |
| :--- | :---: | :---: | :---: | :---: |
| **Total Replayed Events** | 2,000,000 | 2,000,000 | 2,000,000 | **2,000,000** |
| **Total Delivered Packets** | 72,096 | 186,386 | 132,315 | **760,884** |
| **Overall Delivery Ratio (PDR)** | 3.60% | 9.32% *(+5.72 pp)* | 6.62% | **38.04% *(+31.42 pp)*** |
| **Capacity Improvement Factor** | 1.00x | 2.59x | 1.00x | **5.75x** |
| **Emergency Packets Generated** | 966,289 | 966,289 | 966,289 | **966,289** |
| **Emergency Packets Delivered** | 1,848 | 116,591 | 3,667 | **528,935** |
| **Emergency Delivery Ratio** | 0.19% | 12.07% *(+11.88 pp)* | 0.38% | **54.74% *(+54.36 pp)*** |
| **Emergency Delivery Gain** | 1.00x | 63.5x | 1.00x | **144.0x** |
| **Total Co-Channel Collisions** | 1,927,904 | 930,203 | 1,867,685 | **768,060** |
| **Channel Collision Rate** | 96.40% | 46.51% *(-49.89 pp)* | 93.38% | **38.40% *(-54.98 pp)*** |
| **Collisions Prevented** | — | 997,701 *(51.75%)* | — | **1,099,625 *(58.88%)*** |
| **Energy per Delivered Pkt** | 171,161 mJ | 40,017 mJ | 27,170 mJ | **5,991 mJ *(5.99 J)*** |
| **Relative Energy Reduction** | — | -76.62% | — | **-77.95%** |
| **Aggregate Throughput** | 0.039 pkts/s | 0.101 pkts/s | 0.072 pkts/s | **0.414 pkts/s *(331.0 bps)*** |
| **Physical Channel Utilization** | 26.85% | 2.78% | 7.82% | **1.20%** |
| **Master Scheduler Efficiency** | 0.0% | 54.24% | 0.0% | **61.88%** |

---

## Repository Structure

\.
├── code/
│   ├── main.py                      # Simulation runner & multi-process orchestrator
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
├── dataset/                         # Benchmark network telemetry trace files
├── results/
│   ├── files/
│   │   ├── comparison_results.json  # Exact simulation results matrix
│   │   └── simulation_timeseries.json # 100k sliding window timeseries
│   └── graphs/                      # High-resolution vector PDF and PNG figures
├── main.tex                         # Complete Elsevier Ad Hoc Networks LaTeX manuscript
├── references.bib                   # BibTeX bibliography with full DOIs
├── README.md                        # Documentation & setup instructions
└── requirements.txt                 # Python dependencies
\
---

## Quick Start & Installation

### 1. Prerequisites
Ensure you have **Python 3.10+** installed:
\\ash
python --version
\
### 2. Clone the Repository & Install Dependencies
\\ash
git clone https://github.com/YourUsername/RIACC-LoRa-MAC.git
cd RIACC-LoRa-MAC
pip install -r requirements.txt
\
---

## Running the Simulation & Generating Figures

### Execute Full 2.0M-Event 4-Scenario Evaluation:
\\ash
python code/main.py
\
### Generate All 10 Publication-Quality Figures:
\\ash
python code/graph_generator.py
\*Outputs all 10 evaluation plots in vector \.pdf\ and 300 DPI \.png\ directly into esults/graphs/\.*

---

## Citation

\\ibtex
@article{shah2026riacc,
  title={{RIACC}: A Gateway-Assisted Adaptive Communication Management Framework for {LoRa} Networks},
  author={Shah, Safeer Ahmad},
  journal={Ad Hoc Networks},
  year={2026},
  publisher={Elsevier},
  note={Under Review}
}
\
---

## License
This project is licensed under the MIT License.
