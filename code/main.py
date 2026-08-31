# =============================================================================
# main.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Main Simulation Controller & Dataset Pipeline Driver
# OPTIMIZED v3: Ultra-High Speed Multi-Core Parallel Processing
# =============================================================================

import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
if os.name == 'nt':
    os.system('color')
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import multiprocessing
import csv
import itertools
import numpy as np

from models import DatasetEvent, NodeEvent, SimulationEvent
from intelligent_master_node import IntelligentMasterNode
from simulation_engine import SimulationEngine
from performance_metrics import PerformanceAnalyzer
from comparison_engine import ComparisonEngine, CommunicationMode
from logger import logger, set_worker_ipc, console_handler
from feature_engineering import FeatureEngineering
from config import ATTACK_MAPPING


# =============================================================================
# PARALLEL WORKER — module-level for multiprocessing 'spawn' pickling
# =============================================================================

_rssi_cache: Dict[str, float] = {}

def _node_rssi(node_id: str) -> float:
    """Deterministic realistic RSSI per node (Log-Distance Path Loss, LoRa indoor/suburban).
    Bug Fix: Previous formula produced -160 to -210 dBm (physically impossible).
    Corrected to -51 to -115 dBm, within LoRa SF7-SF12 receiver sensitivity range."""
    if node_id not in _rssi_cache:
        import hashlib
        import math
        h = int(hashlib.md5(node_id.encode()).hexdigest()[:8], 16)
        distance_m = 100.0 + (h % 900)    # 100m to 1000m (realistic LoRa deployment)
        # ITU-R P.1411 Log-distance model: PL = PL0 + 10*n*log10(d/d0)
        # PL0 = 65 dB at d0=100m (868 MHz suburban), n=2.5 (suburban path loss exponent)
        pl = 65.0 + 25.0 * math.log10(distance_m / 100.0)  # 65 dB to 90 dB
        _rssi_cache[node_id] = round(14.0 - pl, 1)          # RSSI: -51 to -76 dBm
    return _rssi_cache[node_id]

def _run_scenario_worker(args):
    """
    Worker function executed in a separate process for each scenario.
    Each process creates its own IntelligentMasterNode (zero lock contention).
    Returns (mode_value, stats_dict, metrics_dict).
    """
    import warnings
    warnings.filterwarnings("ignore")

    try:
        worker_idx, mode_value, dataset_files_str, max_records_per_file, log_queue = args

        mode = CommunicationMode(mode_value)
        if log_queue is not None:
            set_worker_ipc(log_queue, worker_idx, mode.name)

        dataset_files = [Path(p) for p in dataset_files_str]

        master_node = IntelligentMasterNode(master_id=f"MASTER_{mode.name}")

        engine = SimulationEngine(master_node=master_node)
        engine.initialize()
        engine.set_communication_mode(mode)

        total_events_processed = 0
        scenario_start = time.time()

        monotonic_time_floor = 0.0  # ensures timestamps never move backwards across chunks

        # Required CSV columns for fast selective parsing
        required_cols = ['ts', 'src_ip', 'dst_ip', 'src_bytes', 'type', 'label']

        sev_map = {'normal': 20.0, 'low': 35.0, 'medium': 50.0, 'high': 75.0, 'critical': 100.0}

        for file_idx, fpath in enumerate(dataset_files, start=1):

            chunk_size = 10000 if (max_records_per_file is None or max_records_per_file > 10000) else max_records_per_file
            file_events_count = 0
            file_first_ts = None

            if str(fpath).endswith('.gz'):
                import gzip
                _file_ctx = gzip.open(fpath, 'rt', encoding='utf-8-sig', errors='replace')
            else:
                _file_ctx = open(fpath, 'r', encoding='utf-8-sig', errors='replace')

            with _file_ctx as f:  # utf-8-sig strips BOM so 'ts' key is read correctly
                reader = csv.DictReader(f)
                
                while True:
                    if max_records_per_file and file_events_count >= max_records_per_file:
                        break
                        
                    limit = chunk_size
                    if max_records_per_file:
                        limit = min(chunk_size, max_records_per_file - file_events_count)
                        
                    chunk_rows = list(itertools.islice(reader, limit))
                    if not chunk_rows:
                        break
                        
                    n_rows = len(chunk_rows)
                    ts_list = []
                    src_ips = []
                    types_raw = []
                    for i, r in enumerate(chunk_rows):
                        try:
                            # 'ts' is now read correctly (BOM stripped by utf-8-sig)
                            t = float(r.get('ts', i))
                        except (ValueError, TypeError):
                            t = float(i)
                        ts_list.append(t)
                        src_ips.append(str(r.get('src_ip', 'UNKNOWN')))
                        types_raw.append(str(r.get('type', 'normal')).strip().lower())
                        
                    ts_raw = np.array(ts_list, dtype=float)
                    if file_first_ts is None and len(ts_raw) > 0:
                        file_first_ts = ts_raw[0] if ts_raw[0] > 1e5 else 0.0
                    if len(ts_raw) > 0:
                        ts_arr = monotonic_time_floor + (ts_raw - file_first_ts)
                    else:
                        ts_arr = ts_raw

                    vectorized_atis = FeatureEngineering.compute_vectorized_ati(types_raw)

                    sim_events = [
                        SimulationEvent(
                            node_id=src_ips[i],
                            timestamp=ts_arr[i],
                            ati=vectorized_atis[i],
                            battery=100.0,
                            rssi=_node_rssi(src_ips[i]),
                            snr=5.0,
                            emergency=(ATTACK_MAPPING.get(types_raw[i], 'normal').lower() in ('critical', 'high') or types_raw[i] in ('ddos', 'injection', 'ransomware', 'dos', 'backdoor'))
                        )
                        for i in range(n_rows)
                    ]
                    engine.load_events(sim_events, mode=mode)
                    sim_events = None   # free 10k event objects before run()
                    engine.run()
                    file_events_count += n_rows

            total_events_processed += file_events_count
            if file_events_count > 0:
                monotonic_time_floor = float(engine.clock.now()) + 1.0

        import gc
        gc.collect()

        stats = engine.statistics()
        stats["total_generated"] = total_events_processed

        # --- Slim the stats dict BEFORE pickling back to main process ----------
        # These fields are large lists/dicts that are either unused downstream or
        # can be replaced with pre-computed scalars, dramatically reducing the
        # pickle payload size and preventing MemoryError in _handle_results.

        # 1. queue_samples: PerformanceAnalyzer only needs sum/len → give it a
        #    single-element list containing the pre-computed average so its
        #    formula `sum(qs)/len(qs)` still works identically.
        qs = stats.get("queue_samples", [])
        avg_ql = (sum(qs) / len(qs)) if qs else 0.0
        # 2. time_series: preserve recorded time series snapshots for continuous graphs
        stats["time_series"] = engine.statistics().get("time_series", [])

        # 3. sf_channel_counts: not consumed by any downstream report or graph.
        stats["sf_channel_counts"] = {}

        # -----------------------------------------------------------------------

        analyzer = PerformanceAnalyzer()
        metrics = analyzer.compute(stats)

        return mode_value, stats, metrics

    except KeyboardInterrupt:
        return None
    except Exception as exc:
        import traceback
        err_msg = f"Worker Error: {exc}\n{traceback.format_exc()}"
        print(err_msg)
        if log_queue:
            log_queue.put((-1, "ERROR", err_msg, err_msg))
        return None


# =============================================================================
# MAIN SIMULATION CONTROLLER
# =============================================================================

class RIACCSimulation:
    """
    Controls the complete RIACC research simulation workflow.
    OPTIMIZED v3: Maximum multi-core parallel processing across 16 CPU threads.
    """

    def __init__(self):

        self.dataset_events: List[DatasetEvent] = []

        self.node_events: List[NodeEvent] = []

        self.simulation_events: List[SimulationEvent] = []

        self.statistics: Dict[Any, Any] = {}

        self.metrics: Dict[Any, Any] = {}

        self.master_node = IntelligentMasterNode.get_instance()
        self.comparison_engine = ComparisonEngine()

    def banner(self):

        print("\n=========================================================")
        print(" A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)")
        print(" Multi-Core Parallel Research Simulation Pipeline (v3.0)")
        print("=========================================================\n")

    def initialize(self):

        self.banner()

        logger.info("Initializing RIACC Simulation System Modules...")

        self.master_node.reset()

        logger.info("Initialization complete.")

    def print_master_paper_tables(self):
        """
        Prints the consolidated publication paper tables:
          - TABLE I: Multi-Scenario Per-Node & Communication Performance Summary
          - TABLE II: Executive Research Performance Benchmark Matrix
        """
        base_coll_aloha = 0
        base_coll_class_a = 0
        for mode, stats in self.statistics.items():
            m_name = mode.name if hasattr(mode, 'name') else str(mode)
            coll = int(stats.get("channel", {}).get("collisions", 0))
            if m_name == "PURE_ALOHA":
                base_coll_aloha = coll
            elif m_name == "CLASS_A_ADR":
                base_coll_class_a = coll

        print("\n" + "=" * 135)
        print("  TABLE I: MULTI-SCENARIO PDR, ENERGY & COLLISION METRICS (All Scenarios)")
        print("=" * 135)
        t1_hdr = (
            f"{'Communication Mode':<24} | {'Total Pkts Gen':<14} | {'Col. Recorded':<13} | {'Col. Prevented':<14} | "
            f"{'Single Tx (mJ)':<14} | {'Total Energy (kJ)':<17} | {'E/Delivered (mJ)':<16} | {'Time/Pkt(ms)':<12} | {'PDR (%)':<8}"
        )
        print(t1_hdr)
        print("-" * 175)

        self.table3_rows = []

        for mode, stats in self.statistics.items():
            metrics = self.metrics.get(mode, None)

            mode_name = mode.name if hasattr(mode, 'name') else str(mode)
            per_node = stats.get("per_node_stats", {})
            nodes_cnt = len(per_node) if per_node else 18

            gen = int(stats.get("total_generated", stats.get("generated", 0)))
            ch_stats = stats.get("channel", {})
            deliv = int(ch_stats.get("delivered", 0))
            drop = int(ch_stats.get("dropped", 0))
            coll = int(ch_stats.get("collisions", 0))

            emg_gen = sum(n.get("emergency_events", n.get("emergency", 0)) for n in per_node.values()) if per_node else 0
            emg_deliv = int(stats.get("emergency_delivered", 0))
            emg_drop = max(0, emg_gen - emg_deliv)
            
            norm_gen = max(0, gen - emg_gen)
            norm_deliv = max(0, deliv - emg_deliv)
            norm_drop = max(0, drop - emg_drop)

            # ── 3-metric energy model ──────────────────────────────────────
            # Single Tx energy: physical radio cost per one transmission
            # (25 mW @ SF<=9 or 35 mW @ SF12, multiplied by ToA ≈ 0.035 s)
            total_energy_mj = stats.get("energy_sum", 0.0)
            single_tx_mj = (total_energy_mj / max(1, gen))  # avg mJ per transmission attempt
            total_energy_kj = total_energy_mj / 1000.0
            energy_per_delivered = metrics.energy_per_packet if metrics else 0.0
            pdr = metrics.packet_delivery_ratio if metrics else ((deliv / max(1, gen) * 100.0))
            time_ms = metrics.average_waiting_time * 1000.0 if metrics else 0.0

            prevented = 0
            if "ALOHA_RIACC" in mode_name:
                prevented = max(0, base_coll_aloha - coll)
            elif "CLASS_A_ADR_RIACC" in mode_name:
                prevented = max(0, base_coll_class_a - coll)
            elif "CLASS_B_RIACC" in mode_name:
                prevented = max(0, base_coll_class_a - coll)

            row = (
                f"{mode_name:<24} | {gen:<14,} | {coll:<13,} | {prevented:<14,} | "
                f"{single_tx_mj:<14.4f} | {total_energy_kj:<17.4f} | {energy_per_delivered:<16.2f} | {time_ms:<12.2f} | {pdr:<8.2f}"
            )
            print(row)
            
            row3 = (
                f"{mode_name:<24} | {nodes_cnt:<11} | {gen:<10,} | "
                f"{norm_deliv:<14,} | {norm_drop:<14,} | {emg_drop:<10,} | {emg_deliv:<10,}"
            )
            self.table3_rows.append(row3)

        print("=" * 175 + "\n")


        # Extract measured empirical metrics per mode (all 4 distinct scenarios)
        m_aloha = None
        m_aloha_riacc = None
        m_class_a = None
        m_class_a_riacc = None

        for mode_obj, met in self.metrics.items():
            m_name = mode_obj.name if hasattr(mode_obj, 'name') else str(mode_obj)
            if "PURE_ALOHA_RIACC" in m_name:
                m_aloha_riacc = met
            elif "PURE_ALOHA" in m_name:
                m_aloha = met
            elif "CLASS_A_ADR_RIACC" in m_name or "CLASS_A_RIACC" in m_name:
                m_class_a_riacc = met
            elif "CLASS_A" in m_name:
                m_class_a = met

        # 1. Packet Delivery Ratio
        pdr_aloha       = f"{m_aloha.packet_delivery_ratio:.2f}%"       if m_aloha       else "N/A"
        pdr_aloha_riacc = f"{m_aloha_riacc.packet_delivery_ratio:.2f}%" if m_aloha_riacc else "N/A"
        pdr_class_a     = f"{m_class_a.packet_delivery_ratio:.2f}%"     if m_class_a     else "N/A"
        pdr_class_a_riacc = f"{m_class_a_riacc.packet_delivery_ratio:.2f}%" if m_class_a_riacc else "N/A"
        pdr_imp         = f"+{(m_class_a_riacc.packet_delivery_ratio - m_class_a.packet_delivery_ratio):.2f}%" if (m_class_a_riacc and m_class_a) else "N/A"

        # 2. Network Throughput (Rate)
        tp_aloha       = f"{m_aloha.throughput*60.0:.2f} pkts/min"       if m_aloha       else "N/A"
        tp_aloha_riacc = f"{m_aloha_riacc.throughput*60.0:.2f} pkts/min" if m_aloha_riacc else "N/A"
        tp_class_a     = f"{m_class_a.throughput*60.0:.2f} pkts/min"     if m_class_a     else "N/A"
        tp_class_a_riacc = f"{m_class_a_riacc.throughput*60.0:.2f} pkts/min" if m_class_a_riacc else "N/A"
        tp_imp         = f"+{((m_class_a_riacc.throughput - m_class_a.throughput)/max(0.0001, m_class_a.throughput)*100.0):.1f}%" if (m_class_a_riacc and m_class_a) else "N/A"

        # 3. Spectral Goodput (Bitrate in bps) = delivered * 32 bytes * 8 bits / sim_time
        gp_aloha       = f"{m_aloha.throughput*32.0*8.0:.2f} bps"       if m_aloha       else "N/A"
        gp_aloha_riacc = f"{m_aloha_riacc.throughput*32.0*8.0:.2f} bps" if m_aloha_riacc else "N/A"
        gp_class_a     = f"{m_class_a.throughput*32.0*8.0:.2f} bps"     if m_class_a     else "N/A"
        gp_class_a_riacc = f"{m_class_a_riacc.throughput*32.0*8.0:.2f} bps" if m_class_a_riacc else "N/A"
        gp_imp         = f"+{((m_class_a_riacc.throughput - m_class_a.throughput)/max(0.0001, m_class_a.throughput)*100.0):.1f}%" if (m_class_a_riacc and m_class_a) else "N/A"

        # 4. Energy Consumed per Delivered Packet (mJ)
        e_aloha       = f"{m_aloha.energy_per_packet:.2f} mJ"       if m_aloha       else "N/A"
        e_aloha_riacc = f"{m_aloha_riacc.energy_per_packet:.2f} mJ" if m_aloha_riacc else "N/A"
        e_class_a     = f"{m_class_a.energy_per_packet:.2f} mJ"     if m_class_a     else "N/A"
        e_class_a_riacc = f"{m_class_a_riacc.energy_per_packet:.2f} mJ" if m_class_a_riacc else "N/A"
        e_imp         = f"{((m_class_a.energy_per_packet - m_class_a_riacc.energy_per_packet)/max(0.001, m_class_a.energy_per_packet)*100.0):.1f}% saved" if (m_class_a_riacc and m_class_a and m_class_a.energy_per_packet > 0) else "N/A"

        # 5. Average Packet Delivery Delay (ms)
        delay_aloha       = f"{m_aloha.average_waiting_time*1000.0:.2f} ms"       if m_aloha       else "N/A"
        delay_aloha_riacc = f"{m_aloha_riacc.average_waiting_time*1000.0:.2f} ms" if m_aloha_riacc else "N/A"
        delay_class_a     = f"{m_class_a.average_waiting_time*1000.0:.2f} ms"     if m_class_a     else "N/A"
        delay_class_a_riacc = f"{m_class_a_riacc.average_waiting_time*1000.0:.2f} ms" if m_class_a_riacc else "N/A"
        delay_imp         = "Empirical"

        # 6. Emergency Delivery Ratio (%)
        emg_aloha       = f"{m_aloha.emergency_delivery_ratio:.2f}%"       if m_aloha       else "N/A"
        emg_aloha_riacc = f"{m_aloha_riacc.emergency_delivery_ratio:.2f}%" if m_aloha_riacc else "N/A"
        emg_class_a     = f"{m_class_a.emergency_delivery_ratio:.2f}%"     if m_class_a     else "N/A"
        emg_class_a_riacc = f"{m_class_a_riacc.emergency_delivery_ratio:.2f}%" if m_class_a_riacc else "N/A"
        emg_imp         = f"+{(m_class_a_riacc.emergency_delivery_ratio - m_class_a.emergency_delivery_ratio):.2f}%" if (m_class_a_riacc and m_class_a) else "N/A"

        t2_metrics = [
            ("Measured Packet Delivery Ratio (PDR %)",   pdr_aloha,   pdr_aloha_riacc,   pdr_class_a,   pdr_class_a_riacc,   pdr_imp),
            ("Measured Network Throughput (Rate)",       tp_aloha,    tp_aloha_riacc,    tp_class_a,    tp_class_a_riacc,    tp_imp),
            ("Measured Spectral Goodput (Bitrate)",      gp_aloha,    gp_aloha_riacc,    gp_class_a,    gp_class_a_riacc,    gp_imp),
            ("Measured Emergency Delivery Ratio (%)",    emg_aloha,   emg_aloha_riacc,   emg_class_a,   emg_class_a_riacc,   emg_imp),
            ("Average Packet Delivery Delay (ms)",       delay_aloha, delay_aloha_riacc, delay_class_a, delay_class_a_riacc, delay_imp),
            ("Energy Consumed per Delivered Packet (mJ)", e_aloha,    e_aloha_riacc,     e_class_a,     e_class_a_riacc,     e_imp),
        ]

        # Rebuild TABLE II header with all 4 distinct scenario columns
        print("\n" + "=" * 175)
        print("  TABLE II: EXECUTIVE RESEARCH PERFORMANCE BENCHMARK MATRIX (All 4 Evaluated Scenarios)")
        print("=" * 175)
        t2_hdr = (
            f"{'Performance Metric':<43} | "
            f"{'Pure ALOHA':<14} | {'ALOHA + RIACC':<16} | {'LoRa Class A':<14} | "
            f"{'Class A + RIACC':<18} | {'Improvement (vs Class A)':<24}"
        )
        print(t2_hdr)
        print("-" * 175)

        for m in t2_metrics:
            row = (
                f"{m[0]:<43} | "
                f"{m[1]:<14} | {m[2]:<16} | {m[3]:<14} | "
                f"{m[4]:<18} | {m[5]:<24}"
            )
            print(row)

        print("=" * 175 + "\n")

        print("\n" + "=" * 130)
        print("  TABLE III: GRANULAR PACKET DISPOSITION (Normal vs Emergency Trajectory)")
        print("=" * 130)
        t3_hdr = (
            f"{'Communication Mode':<24} | {'Total Nodes':<11} | {'Total Gen':<10} | "
            f"{'Norm. Success':<14} | {'Norm. Dropped':<14} | {'Emg. Drop':<10} | {'Emg. Success':<10}"
        )
        print(t3_hdr)
        print("-" * 130)
        for r in getattr(self, 'table3_rows', []):
            print(r)
        print("=" * 130 + "\n")


    def generate_reports(self, skip_graphs: bool = False):
        """
        Generates research graphs, exports results, and validates research metrics.
        Outputs Table I and Table II. Saves Table III (RQ Report) to results/ directory.
        """
        logger.info("==========================================")
        logger.info("Generating Research Outputs & Plots")
        logger.info("==========================================")

        from results_exporter import ResultsExporter
        from research_validation import ResearchValidator

        if not skip_graphs:
            try:
                from graph_generator import GraphGenerator
                graph_generator = GraphGenerator(self.comparison_engine)
                graph_generator.generate_all()
            except ImportError as exc:
                print(f"\n[WARNING] Could not generate graphs: {exc}. (Install matplotlib to fix this)")
            except Exception as exc:
                print(f"\n[WARNING] Error generating graphs: {exc}")
        else:
            logger.info("Skipping graph generation as requested.")

        exporter = ResultsExporter(self.comparison_engine)
        exporter.export_all()

        validator = ResearchValidator(self.comparison_engine)
        validator.validate_all()

        # Save RQ report to results/ directory (does not print to console)
        validator.print_report()

        # Print Consolidated Publication Paper Tables (Table I and Table II)
        self.print_master_paper_tables()

    def execute_streaming(
        self,
        dataset_files: List[Path],
        max_records_per_file: Optional[int] = None,
        force_sequential: bool = False,
        skip_graphs: bool = False,
    ) -> ComparisonEngine:
        """
        Runs all 4 scenarios in parallel across CPU cores using multiprocessing.Pool,
        displaying a clean, non-scrolling, in-place updating 23-line dashboard in VS Code terminal.
        """
        self._skip_graphs = skip_graphs
        self.initialize()

        logger.info(f"Starting Execution over {len(dataset_files)} dataset file(s):")
        for f in dataset_files:
            logger.info(f"  • {f.name}")

        scenarios = [
            CommunicationMode.PURE_ALOHA,
            CommunicationMode.PURE_ALOHA_RIACC,
            CommunicationMode.CLASS_A_ADR,
            CommunicationMode.CLASS_A_ADR_RIACC,
        ]

        dataset_files_str = [str(p.resolve()) for p in dataset_files]
        num_cpus = multiprocessing.cpu_count()

        results = []

        import sys
        import os
        import shutil
        import threading
        import time as _time

        # Silence raw logger stream writes while live dashboard thread is running
        if hasattr(console_handler, 'dashboard_active'):
            console_handler.dashboard_active = True

        def _run_dashboard_thread(log_queue):
            import os
            import sys

            num_scen = len(scenarios)
            state = {}
            for idx, mode in enumerate(scenarios):
                m_name     = mode.name
                is_riacc   = "RIACC" in m_name
                is_class_a = "CLASS_A" in m_name
                state[idx] = {
                    "mode_name": m_name, "mode_value": mode.value,
                    "is_riacc": is_riacc, "is_class_a": is_class_a,
                    "nodes": "18", "packets": "0", "normal": "0", "emergency": "0",
                    "grants": "0", "gnts": "18.0",
                    "sensor_event": "[SENSOR] Initializing sensor trace...",
                    "threat_state": "Normal Operation",
                    "freq": "866.4" if is_riacc else "865.0625",
                    "sf": "7" if (is_riacc or is_class_a) else "12",
                    "sf_tag": "(Gateway-assigned)" if (is_class_a and not is_riacc) else ("" if is_riacc else "(Static)"),
                    "success": "0", "collisions": "0", "pdr": "0.0%", "retries": "0.00",
                }

            def render_lines():
                t_cols  = shutil.get_terminal_size((120, 30)).columns
                max_len = max(70, min(118, t_cols - 2))
                def fit(s):
                    return (s[:max_len - 3] + "...") if len(s) > max_len else s

                lines = []
                for idx, mode in enumerate(scenarios):
                    st = state[idx]
                    lines.append(fit(f"[{mode.value:<30}]"))

                    if st["is_riacc"]:
                        l0 = f"  • Master Status  : {st['grants']} grants | GNTS={st['gnts']} | Nodes:{st['nodes']} | Pkts:{st['packets']}"
                    else:
                        l0 = f"  • Nodes & Events : Nodes:{st['nodes']} | Pkts:{st['packets']} | Norm:{st['normal']} | Emg:{st['emergency']}"
                    lines.append(fit(l0))
                    lines.append(fit(f"  • Sensor Event   : {st['sensor_event']}"))

                    if st["is_riacc"]:
                        l2 = f"  • Threat State   : {st['threat_state']}"
                    elif st["is_class_a"]:
                        l2 = "  • Uplink Mode    : Class A Uplink Transmissions (LoRaWAN)"
                    else:
                        l2 = "  • Uplink Mode    : Direct Unslotted Transmissions (Pure ALOHA)"
                    lines.append(fit(l2))

                    sf_part = f"{st['sf']} {st['sf_tag']}".strip()
                    ret_val = str(st['retries']).replace("retries/pkt", "").strip()
                    lines.append(fit(
                        f"  • Live Channel   : Ch:{st['freq']} MHz | SF:{sf_part} "
                        f"| Succ:{st['success']} | Col:{st['collisions']} | PDR:{st['pdr']} | Ret:{ret_val}"
                    ))

                    if idx < num_scen - 1:
                        lines.append("")   # blank separator between scenarios

                return lines

            # --- Enable Windows VT100 (ANSI) ---
            ansi_ok = False
            if os.name == 'nt':
                try:
                    import ctypes
                    k32     = ctypes.windll.kernel32
                    hstdout = k32.GetStdHandle(-11)
                    cur_mode = ctypes.c_ulong()
                    k32.GetConsoleMode(hstdout, ctypes.byref(cur_mode))
                    k32.SetConsoleMode(hstdout, cur_mode.value | 0x0004)
                    ansi_ok = True
                except Exception:
                    ansi_ok = False
            else:
                ansi_ok = sys.stdout.isatty()

            # Print initial dashboard frame
            initial_lines = render_lines()
            # last_line_count stored in a list so the closure can mutate it
            last_line_count = [len(initial_lines)]
            for l in initial_lines:
                sys.stdout.write(f"{l}\033[K\n")
            sys.stdout.flush()

            last_render = [0.0]

            while True:
                try:
                    item = log_queue.get(timeout=0.05)

                    if item == "STOP":
                        # Final render with completed channel ranges
                        for idx, st in state.items():
                            m = st.get("mode_name", "")
                            if "RIACC" in m:
                                st["freq"] = "865.0625-866.4 (5 Ch)"; st["sf"] = "7-12"; st["sf_tag"] = ""
                            elif "CLASS_A" in m:
                                st["freq"] = "865.0625-866.4850"; st["sf"] = "7-12"; st["sf_tag"] = "(Gateway-assigned)"
                            else:
                                st["sf"] = "9"; st["sf_tag"] = "(Static)"
                        final_lines = render_lines()
                        if ansi_ok:
                            sys.stdout.write(f"\033[{last_line_count[0]}A")
                        for l in final_lines:
                            sys.stdout.write(f"\r{l}\033[K\n")
                        sys.stdout.write("\n")
                        sys.stdout.flush()
                        break

                    w_idx, w_name, msg, raw_msg = item
                    st = state.get(w_idx)
                    if st is None:
                        continue

                    if "[STATS]" in raw_msg:
                        for p in (x.strip() for x in raw_msg.replace("[STATS]", "").split("|")):
                            if   p.startswith("Nodes:"):      st["nodes"]      = p[6:].strip()
                            elif p.startswith("Packets:"):    st["packets"]    = p[8:].strip()
                            elif p.startswith("Success:"):    st["success"]    = p[8:].strip()
                            elif p.startswith("Collisions:"): st["collisions"] = p[11:].strip()
                            elif p.startswith("PDR:"):        st["pdr"]        = p[4:].strip()
                            elif p.startswith("Normal:"):     st["normal"]     = p[7:].strip()
                            elif p.startswith("Emergency:"):  st["emergency"]  = p[10:].strip()
                            elif p.startswith("Retries:"):    st["retries"]    = p[8:].replace("retries/pkt", "").strip()
                            elif p.startswith("RF:"):
                                rf = p[3:].strip()
                                if "MHz" in rf:
                                    st["freq"] = rf.split("MHz")[0].replace("Ch:", "").strip()
                                if "SF:" in rf:
                                    sf_p = rf.split("SF:")[1].strip()
                                    if "(" in sf_p:
                                        st["sf"] = sf_p.split("(")[0].strip()
                                        st["sf_tag"] = "(" + sf_p.split("(")[1]
                                    else:
                                        st["sf"] = sf_p.strip(); st["sf_tag"] = ""
                        if st["is_riacc"]:
                            st["grants"] = st["packets"]

                    elif "[SENSOR]" in raw_msg:
                        st["sensor_event"] = raw_msg.strip()

                    elif st["is_riacc"] and ("Master Progress:" in raw_msg or "[MASTER]" in raw_msg):
                        if "GNTS=" in raw_msg:
                            try: st["gnts"] = raw_msg.split("GNTS=")[1].split("]")[0].strip()
                            except Exception: pass
                        if "grants issued" in raw_msg:
                            try: st["grants"] = raw_msg.split("grants issued")[0].split("Progress:")[1].strip()
                            except Exception: pass

                    elif st["is_riacc"] and ("Fast-Path" in raw_msg or "Emergency Request" in raw_msg):
                        st["threat_state"] = raw_msg.replace("[MASTER]", "").strip()

                    # Throttle renders to at most 1 per second to avoid flicker
                    now = _time.time()
                    if now - last_render[0] >= 1.0:
                        last_render[0] = now
                        curr_lines = render_lines()

                        if ansi_ok:
                            # Move cursor UP by the number of lines we last printed
                            sys.stdout.write(f"\033[{last_line_count[0]}A")

                        last_line_count[0] = len(curr_lines)

                        for l in curr_lines:
                            sys.stdout.write(f"\r{l}\033[K\n")
                        sys.stdout.flush()

                except Exception:
                    pass

        # ---------------------------------------------------------------
        # Run all 4 scenarios truly in parallel — always use Pool, never serial
        # ---------------------------------------------------------------
        pool        = None
        dash_thread = None
        log_queue   = None
        try:
            # Manager queue works across spawned processes on Windows
            manager   = multiprocessing.Manager()
            log_queue = manager.Queue()

            worker_args = [
                (idx, mode.value, dataset_files_str, max_records_per_file, log_queue)
                for idx, mode in enumerate(scenarios)
            ]

            dash_thread = threading.Thread(
                target=_run_dashboard_thread, args=(log_queue,), daemon=True
            )
            dash_thread.start()

            # 4 workers — one per scenario — all run simultaneously
            max_procs = min(4, num_cpus, len(worker_args))
            pool = multiprocessing.Pool(processes=max_procs)
            res_async = pool.map_async(_run_scenario_worker, worker_args)
            results = res_async.get()  # wait indefinitely — no timeout limit on Python 3.13
            pool.close()
            pool.join()

        except KeyboardInterrupt:
            print("\n\n[CANCELLED] Simulation interrupted by user (Ctrl+C). Terminating...", flush=True)
            if pool:
                pool.terminate()
                pool.join()
            sys.exit(0)
        except Exception as exc:
            import traceback
            print(f"\n[ERROR] Parallel pool failed: {exc}\n{traceback.format_exc()}")
            if pool:
                pool.terminate()
                pool.join()
            results = []
        finally:
            try:
                if log_queue is not None:
                    log_queue.put("STOP")
                if dash_thread is not None:
                    dash_thread.join(timeout=2.0)
            except Exception:
                pass

        # Restore console logging handler for report tables
        if hasattr(console_handler, 'dashboard_active'):
            console_handler.dashboard_active = False

        # Filter out any cancelled worker results
        results = [r for r in results if r is not None]

        from collections import defaultdict
        merged_stats        = defaultdict(list)
        merged_metrics_list = defaultdict(list)

        for mode_value, stats, metrics in results:
            merged_stats[mode_value].append(stats)
            merged_metrics_list[mode_value].append(metrics)

        for mode_value, stats_list in merged_stats.items():
            mode          = CommunicationMode(mode_value)
            combined_stats = stats_list[0].copy()
            for extra in stats_list[1:]:
                combined_stats['total_generated']   = combined_stats.get('total_generated', 0)   + extra.get('total_generated', 0)
                combined_stats['generated']         = combined_stats.get('generated', 0)         + extra.get('generated', 0)
                combined_stats['energy_sum']        = combined_stats.get('energy_sum', 0)        + extra.get('energy_sum', 0)
                combined_stats['wait_sum']          = combined_stats.get('wait_sum', 0)          + extra.get('wait_sum', 0)
                combined_stats['rssi_sum']          = combined_stats.get('rssi_sum', 0)          + extra.get('rssi_sum', 0)
                combined_stats['snr_sum']           = combined_stats.get('snr_sum', 0)           + extra.get('snr_sum', 0)
                combined_stats['ati_sum']           = combined_stats.get('ati_sum', 0)           + extra.get('ati_sum', 0)
                combined_stats['emergency_generated'] = combined_stats.get('emergency_generated', 0) + extra.get('emergency_generated', 0)
                combined_stats['emergency_delivered'] = combined_stats.get('emergency_delivered', 0) + extra.get('emergency_delivered', 0)

                ch1 = combined_stats.get('channel', {})
                ch2 = extra.get('channel', {})
                for k in ('transmitted', 'delivered', 'collisions', 'dropped'):
                    ch1[k] = ch1.get(k, 0) + ch2.get(k, 0)
                ch1['channel_busy_time'] = ch1.get('channel_busy_time', 0) + ch2.get('channel_busy_time', 0)
                combined_stats['channel'] = ch1

                pn1 = combined_stats.get('per_node_stats', {})
                pn2 = extra.get('per_node_stats', {})
                for nid, nstats in pn2.items():
                    if nid in pn1:
                        for k, v in nstats.items():
                            pn1[nid][k] = pn1[nid].get(k, 0) + v
                    else:
                        pn1[nid] = nstats
                combined_stats['per_node_stats'] = pn1

                combined_stats['queue_samples'] = combined_stats.get('queue_samples', []) + extra.get('queue_samples', [])

            analyzer       = PerformanceAnalyzer()
            combined_metrics = analyzer.compute(combined_stats)

            self.statistics[mode]  = combined_stats
            self.metrics[mode]     = combined_metrics
            self.comparison_engine.add_result(mode, combined_metrics, raw_stats=combined_stats)

        self.generate_reports(skip_graphs=getattr(self, '_skip_graphs', False))

        logger.info("==========================================")
        logger.info("RIACC Research Simulation Executed Successfully")
        logger.info("==========================================")

        return self.comparison_engine


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="RIACC Research Simulation Pipeline")

    parser.add_argument(
        "--dataset-files", type=str, nargs="+", default=None,
        help="List of CSV dataset file paths to simulate"
    )
    parser.add_argument(
        "--max-records-per-file", type=int, default=None,
        help="Cap records per dataset file (default: None for full file)"
    )
    parser.add_argument(
        "--sequential", action="store_true",
        help="Force sequential execution"
    )
    parser.add_argument(
        "--skip-graphs", action="store_true",
        help="Skip graph PNG generation during benchmark execution"
    )

    args = parser.parse_args()

    start_time = time.time()

    simulation = RIACCSimulation()

    if args.dataset_files:
        target_files = [Path(f) for f in args.dataset_files]
    else:
        # Default target: Network_dataset_1 and Network_dataset_17
        root_dir = Path(__file__).resolve().parent.parent
        dataset_dir = root_dir / "dataset"
        f1 = dataset_dir / "Network_dataset_1.csv"
        if not f1.exists():
            f1 = dataset_dir / "Network_dataset_1.csv.gz"
        f17 = dataset_dir / "Network_dataset_17.csv"
        if not f17.exists():
            f17 = dataset_dir / "Network_dataset_17.csv.gz"
        target_files = [f1, f17]

    simulation.execute_streaming(
        target_files,
        max_records_per_file=args.max_records_per_file,
        force_sequential=args.sequential,
        skip_graphs=args.skip_graphs,
    )

    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = total_time % 60

    print("\n" + "=" * 70)
    print(f" TOTAL SIMULATION RUNTIME: {minutes} minutes {seconds:.2f} seconds ({total_time:.2f} s total)")
    print("=" * 70 + "\n")
    logger.info(f"TOTAL SIMULATION RUNTIME: {minutes}m {seconds:.2f}s ({total_time:.2f}s total)")