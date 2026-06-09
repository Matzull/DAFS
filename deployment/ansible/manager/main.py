#!/usr/bin/env python3
"""DAFS Cluster Manager — PyQt6 GUI."""

from __future__ import annotations

import json
import os
import queue
import shlex
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from PyQt6.QtCore import QTimer
from PyQt6.QtWidgets import QSizePolicy
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

_raw_root = Path(__file__).resolve().parents[3]
raw = str(_raw_root)
if "wsl.localhost" in raw.lower():
    # Windows UNC path — use cwd to get correct Unix case
    cwd = Path.cwd()
    # Walk up from cwd to find repo root (contains deployment/ansible/manager)
    _unix_root = str(cwd)
    for parent in cwd.parents:
        if (parent / "deployment" / "ansible" / "manager").exists():
            _unix_root = str(parent)
            break
    REPO_ROOT = _raw_root
    UNIX_ROOT = _unix_root
else:
    REPO_ROOT = _raw_root
    UNIX_ROOT = str(_raw_root)
PLAYBOOK_PATH = REPO_ROOT / "deployment" / "ansible" / "site.yml"
GENERATED_DIR = REPO_ROOT / "deployment" / "ansible" / "generated"
CONFIG_PATH = REPO_ROOT / "deployment" / "ansible" / "cluster_config.json"
INVENTORY_PATH = GENERATED_DIR / "cluster_inventory.ini"

# ── Shared helpers ──────────────────────────────────────────────────────

def load_config() -> dict[str, Any] | None:
    if not CONFIG_PATH.exists():
        return None
    cfg = json.loads(CONFIG_PATH.read_text())
    # Fix any UNC/stale paths in the config immediately on load
    for host in cfg.get("hosts", []):
        if "\\" in host.get("repo_root", "") or not Path(host["repo_root"]).exists():
            host["repo_root"] = _fix_repo_root(host["repo_root"])
    return cfg


def save_config(cfg: dict[str, Any]) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def inv_line(host: dict) -> str:
    n, c = host["name"], host["connection"]
    if c == "local":
        return f"{n} ansible_connection=local ansible_python_interpreter=auto_silent"
    base = f"{n} ansible_host={host['host']}"
    if host.get("user"):
        base += f" ansible_user={host['user']}"
    return base + " ansible_connection=ssh"


def write_generated(config: dict) -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    lines = ["[cluster]"]
    for host in config["hosts"]:
        lines.append(inv_line(host))
        vp = GENERATED_DIR / f"vars-{host['name']}.json"
        vp.write_text(json.dumps({
            "dafs_repo_root": host["repo_root"],
            "dafs_network_name": config.get("dafs_network_name", "dafs-net"),
            "auditing_network_name": config.get("auditing_network_name", "elastic"),
            "enable_auditing": host["enable_auditing"],
            "enable_masters": bool(host["masters"]),
            "enable_datanodes": bool(host["datanodes"]),
            "master_cluster_members": config.get("master_cluster_members", []),
            "masters": host["masters"],
            "datanodes": host["datanodes"],
        }, indent=2))
    INVENTORY_PATH.write_text("\n".join(lines) + "\n")


def destroy_script(host: dict) -> str:
    r = shlex.quote(_fix_repo_root(host["repo_root"]))
    c = ["set +e"]
    if host["enable_auditing"]:
        c.append(f"docker compose -p dafs-auditing -f {r}/Auditing/docker-compose.yml down --volumes --remove-orphans --rmi local || true")
    for m in host["masters"]:
        c.append(f"docker compose -p {shlex.quote(m['compose_project'])} -f {r}/deployment/docker-compose.master.yml down --volumes --remove-orphans --rmi local || true")
    for d in host["datanodes"]:
        c.append(f"docker compose -p {shlex.quote(d['compose_project'])} -f {r}/deployment/docker-compose.datanode.yml down --volumes --remove-orphans --rmi local || true")
    c += [
        "docker rm -f grafana ai-assistant-backend >/dev/null 2>&1 || true",
        "docker network rm dafs-net >/dev/null 2>&1 || true",
        "docker network rm elastic >/dev/null 2>&1 || true",
        f"rm -rf {r}/Auditing/certs {r}/Auditing/esdata01 {r}/Auditing/grafana/storage",
    ]
    return "\n".join(c)


def _p(path: Path) -> str:
    """Convert a Path to string, converting UNC to Unix on Windows."""
    s = str(path)
    if _is_windows_unc():
        return _win_to_unix(s)
    return s


def _win_to_unix(p: str) -> str:
    """Convert a Windows or UNC path to WSL Unix path."""
    p = str(p).replace("\\", "/")
    parts = p.split("/")
    for i, part in enumerate(parts):
        if part.lower() == "wsl.localhost":
            after = i + 2
            if after < len(parts):
                return "/" + "/".join(parts[after:])
            break
        if part and part.endswith(":"):
            drive = part[0].lower()
            return "/mnt/" + drive + "/" + "/".join(parts[i + 1:])
    return p


def _is_windows_unc() -> bool:
    """Check if we're on Windows with UNC paths (WSL)."""
    return sys.platform == "win32"


def _run_in_wsl(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a command, routing through wsl on Windows if needed."""
    if not _is_windows_unc():
        return subprocess.run(cmd, capture_output=True, text=True)
    # On Windows WSL: strip outer "bash", "-lc" since wsl gives us bash
    cmd_str = cmd[2] if len(cmd) >= 2 and cmd[0] == "bash" and cmd[1] == "-lc" else " ".join(shlex.quote(c) for c in cmd)
    return subprocess.run(["wsl", "bash", "-lc", cmd_str], capture_output=True, text=True)


def run_cmd(cmd: list[str], timeout: int = 600) -> str:
    try:
        r = _run_in_wsl(cmd)
        return r.stdout + r.stderr or f"(exit {r.returncode})"
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    except Exception as e:
        return f"ERROR: {e}"


# ── Log queue (thread-safe) ────────────────────────────────────────────

_log_queue: queue.Queue = queue.Queue()


def bg(fn, *a, **kw):
    def _print(text: str) -> None:
        _log_queue.put(text)

    def _target():
        _print(f"▶ {fn.__name__}")
        try:
            r = fn(_print, *a, **kw)
            if isinstance(r, str):
                _print(r)
            else:
                _print("✓ done")
        except Exception as e:
            _print(f"✗ ERROR: {e}")

    threading.Thread(target=_target, daemon=True).start()


# ── Operations ──────────────────────────────────────────────────────────

def op_init(p, machines: list[dict]):
    hc, mcm, mid, did = [], [], 0, 0
    for mac in machines:
        ms, ds = [], []
        for _ in range(mac["master_count"]):
            nid, cn, sp = mid, f"master{mid}", 9090 + mid
            cc = REPO_ROOT / "services" / "master" / f"config-node{nid}.toml"
            cp = f"../services/master/config-node{nid}.toml" if cc.exists() else "../services/master/config.toml"
            eh = mac.get("advertise_host") or mac["host"]
            ms.append({"compose_project": f"dafs-master-{nid}", "container_name": cn, "node_id": nid,
                       "raft_bootstrap": nid == 0, "config_path": cp, "host_port": sp, "service_port": sp,
                       "listen_address": f"[::]:{sp}", "wal_path": f"/app/data/master{nid}.wal",
                       "auth_db_path": f"/app/data/users{nid}.db", "metadata_host": eh, "raft_host": eh})
            mcm.append(ms[-1].copy())
            mid += 1
        for _ in range(mac["datanode_count"]):
            ds.append({"compose_project": f"dafs-datanode-{did}", "config_path": "../services/datanode/config.toml",
                       "host_port": str(8080 + _), "container_name": f"dafs-datanode-{did}",
                       "rust_log": "info", "listen_address": "[::]:8080"})
            did += 1
        hc.append({"name": mac["name"], "host": mac["host"], "connection": mac["connection"],
                   "user": mac.get("user") or None,
                   "enable_auditing": mac["enable_auditing"], "advertise_host": mac.get("advertise_host", ""),
                   "masters": ms, "datanodes": ds,
                   "repo_root": _fix_repo_root(UNIX_ROOT or str(REPO_ROOT))})
    save_config({"version": 1, "dafs_network_name": "dafs-net", "auditing_network_name": "elastic",
                 "master_cluster_members": mcm, "hosts": hc})
    write_generated(load_config())
    p(f"Configured: {mid} master(s), {did} datanode(s), {len(hc)} host(s).")


def op_build(p):
    cfg = load_config()
    if not cfg:
        p("ERROR: No cluster config.")
        return
    for h in cfg["hosts"]:
        if h["connection"] != "local":
            p(f"SKIP {h['name']} (remote not supported)")
            continue
        r = _fix_repo_root(h["repo_root"])
        bp = "DOCKER_BUILDKIT=1 BUILDKIT_PROGRESS=plain "
        cmds = []
        if h["enable_auditing"]:
            cmds.append(f"{bp}docker compose -p dafs-auditing -f {r}/Auditing/docker-compose.yml build --progress=plain")
        if h["masters"]:
            cmds.append(f"{bp}docker compose -f {r}/deployment/docker-compose.master.yml build --progress=plain")
        if h["datanodes"]:
            cmds.append(f"{bp}docker compose -f {r}/deployment/docker-compose.datanode.yml build --progress=plain")
        for c in cmds:
            p(f"$ {c}")
            p(run_cmd(["bash", "-lc", c], 1200))


def op_deploy(p, bp: str):
    cfg = load_config()
    if not cfg:
        p("ERROR: No cluster config.")
        return
    for h in cfg["hosts"]:
        r = _fix_repo_root(h["repo_root"])
        vp = GENERATED_DIR / f"vars-{h['name']}.json"
        cmd = ["ansible-playbook", "-vv", "-i", _p(INVENTORY_PATH), "-l", h["name"],
               _p(PLAYBOOK_PATH), "-e", f"@{_p(vp)}", "-e", "dafs_build_images=false"]
        if bp:
            cmd += ["-e", json.dumps({"ansible_become_password": bp, "logrotate_sudo_password": bp})]
        p(f"$ {' '.join(cmd)}")
        p(run_cmd(cmd, 600))


def _fix_repo_root(r: str) -> str:
    """Resolve a Unix path. Handles UNC, lowercase, None, empty."""
    if r is None or r.strip() in ("", "None"):
        r = ""
    # UNC paths with backslashes
    if "\\" in r:
        return _win_to_unix(r)
    # Non-existent or empty path → walk from cwd
    if not r or not Path(r).exists():
        base = str(Path.cwd())
        if "\\" in base:
            base = _win_to_unix(base)
        base_p = Path(base) if base.startswith("/") else Path.cwd()
        for parent in base_p.parents:
            if (parent / "deployment" / "ansible" / "manager").exists():
                return str(parent)
    return r


def op_compose(p, action: str):
    cfg = load_config()
    if not cfg:
        p("ERROR: No cluster config.")
        return
    for h in cfg["hosts"]:
        if h["connection"] != "local":
            continue
        r = _fix_repo_root(h["repo_root"])
        cs = []
        if h["enable_auditing"]:
            cs.append(f"docker compose -p dafs-auditing -f {r}/Auditing/docker-compose.yml {action}")
        for m in h["masters"]:
            cs.append(f"docker compose -p {m['compose_project']} -f {r}/deployment/docker-compose.master.yml {action}")
        for d in h["datanodes"]:
            cs.append(f"docker compose -p {d['compose_project']} -f {r}/deployment/docker-compose.datanode.yml {action}")
        s = " && ".join(c for c in cs if c)
        if s:
            p(f"$ {s}")
            p(run_cmd(["bash", "-lc", s]))


def op_clean(p):
    rem = []
    for pth in [CONFIG_PATH, GENERATED_DIR]:
        if pth.exists():
            (shutil.rmtree if pth.is_dir() else pth.unlink)(pth)
            rem.append(str(pth))
    p("Removed: " + ", ".join(rem) if rem else "Nothing to clean.")


def op_destroy(p):
    cfg = load_config()
    if not cfg:
        p("ERROR: No cluster config.")
        return
    for h in cfg["hosts"]:
        h["repo_root"] = _fix_repo_root(h["repo_root"])
        p(f"--- Destroying {h['name']} ---")
        p(run_cmd(["bash", "-lc", destroy_script(h)], 300))
    for pth in [CONFIG_PATH, GENERATED_DIR]:
        if pth.exists():
            (shutil.rmtree if pth.is_dir() else pth.unlink)(pth)
    p("✓ Cluster destroyed.")


# ── Main Window ──────────────────────────────────────────────────────────

STYLE = """
QMainWindow, QWidget { background-color: #11111b; }
QTabWidget::pane { border: none; background: #181825; }
QTabBar::tab { background: #181825; color: #a6adc8; padding: 8px 18px; border: none; font-size: 12px; }
QTabBar::tab:selected { color: #cba6f7; border-bottom: 2px solid #cba6f7; }
QTabBar::tab:hover { color: #cdd6f4; }
QLabel { color: #cdd6f4; }
QPushButton {
    background: #313244; color: #cdd6f4; border: none; border-radius: 5px;
    padding: 6px 14px; font-size: 12px; min-height: 24px;
}
QPushButton:hover { background: #45475a; }
QPushButton:pressed { background: #585b70; }
QLineEdit, QSpinBox, QComboBox {
    background: #313244; color: #cdd6f4; border: 1px solid #45475a;
    border-radius: 4px; padding: 4px 6px; font-size: 12px;
}
QCheckBox { color: #cdd6f4; }
"""


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DAFS Cluster Manager")
        self.setMinimumSize(780, 480)
        self.resize(780, 480)
        self.setStyleSheet(STYLE)

        cw = QWidget()
        self.setCentralWidget(cw)
        lo = QVBoxLayout(cw)
        lo.setContentsMargins(0, 0, 0, 0)
        lo.setSpacing(0)

        # Header
        hdr = QLabel("DAFS Cluster Manager")
        hdr.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px 16px; background: #181825; color: #cba6f7;")
        lo.addWidget(hdr)

        self.tabs = QTabWidget()
        lo.addWidget(self.tabs)

        self._log_boxes: dict[str, QPlainTextEdit] = {}

        self._build_dashboard()
        self._build_wizard()
        self._build_build()
        self._build_deploy()
        self._build_compose()
        self._build_logs()
        self._build_destroy()

        # Poll log queue every 200ms
        self._log_timer = QTimer()
        self._log_timer.timeout.connect(self._drain_logs)
        self._log_timer.start(200)

        self.wiz_widgets: list[list] = []
        self._wiz_add()
        self._refresh_dashboard()

    # ── Logging ──────────────────────────────────────────────────────

    def _register_log(self, key: str, w: QPlainTextEdit):
        self._log_boxes[key] = w

    def _drain_logs(self):
        while True:
            try:
                text = _log_queue.get_nowait()
                for box in self._log_boxes.values():
                    box.appendPlainText(text)
                    sb = box.verticalScrollBar()
                    if sb:
                        sb.setValue(sb.maximum())
            except queue.Empty:
                break

    # ── Widget helpers ────────────────────────────────────────────────

    def _label(self, text: str, bold: bool = False, size: int = 10, color: str | None = None) -> QLabel:
        lb = QLabel(text)
        f = lb.font()
        f.setPointSize(size)
        if bold:
            f.setBold(True)
        lb.setFont(f)
        if color:
            lb.setStyleSheet(f"color: {color};")
        return lb

    def _btn(self, text: str, slot) -> QPushButton:
        b = QPushButton(text)
        b.clicked.connect(slot)
        return b

    def _log_widget(self) -> QPlainTextEdit:
        w = QPlainTextEdit()
        w.setReadOnly(True)
        w.setFont(QFont("JetBrains Mono", 9))
        w.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        w.setStyleSheet("background: #1e1e2e; color: #a6adc8; border: 1px solid #313244; border-radius: 5px; padding: 6px;")
        return w

    def _tab(self, title: str, layout: QVBoxLayout):
        w = QWidget()
        w.setLayout(layout)
        self.tabs.addTab(w, title)

    # ── Dashboard ────────────────────────────────────────────────────

    def _build_dashboard(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(10)

        lo.addWidget(self._label("Cluster Status", bold=True, size=14))

        stats = QHBoxLayout()
        stat_widgets = {}
        for name, key in [("Hosts", "H"), ("Masters", "M"), ("Datanodes", "D"), ("Auditing", "A")]:
            col = QVBoxLayout()
            col.setSpacing(2)
            col.addWidget(self._label(name, size=9, color="#6c7086"))
            lb = self._label("0", bold=True, size=22, color="#a6e3a1")
            col.addWidget(lb)
            stats.addLayout(col)
            stat_widgets[key] = lb
        lo.addLayout(stats)
        self._stat = stat_widgets

        lo.addWidget(self._label("Configuration", bold=True, size=11))
        self._dash_info = QPlainTextEdit()
        self._dash_info.setReadOnly(True)
        self._dash_info.setFont(QFont("JetBrains Mono", 9))
        self._dash_info.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._dash_info.setStyleSheet("background: #1e1e2e; color: #a6adc8; border: 1px solid #313244; border-radius: 5px; padding: 6px;")
        lo.addWidget(self._dash_info, 1)
        lo.addWidget(self._btn("Refresh", self._refresh_dashboard))
        self._tab("Dashboard", lo)

    def _refresh_dashboard(self):
        cfg = load_config()
        if not cfg:
            self._stat["H"].setText("0")
            self._stat["M"].setText("0")
            self._stat["D"].setText("0")
            self._stat["A"].setText("No")
            self._dash_info.setPlainText("No cluster configured. Use Init Wizard.")
            return
        h = cfg["hosts"]
        self._stat["H"].setText(str(len(h)))
        self._stat["M"].setText(str(len(cfg.get("master_cluster_members", []))))
        self._stat["D"].setText(str(sum(len(x["datanodes"]) for x in h)))
        self._stat["A"].setText("Yes" if any(x["enable_auditing"] for x in h) else "No")
        self._dash_info.setPlainText(json.dumps(cfg, indent=2))

    # ── Wizard ───────────────────────────────────────────────────────

    def _build_wizard(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Cluster Initialization Wizard", bold=True, size=14))
        desc = self._label(
            "Define one line per machine in your cluster. "
            "Each machine can run masters (Raft metadata servers), datanodes (storage), "
            "and optionally the auditing stack (Elasticsearch + Grafana). "
            "The first machine starts with 1 master + auditing enabled by default.",
            size=9,
        )
        desc.setWordWrap(True)
        lo.addWidget(desc)

        self.wiz_scroll = QWidget()
        self.wiz_scroll_lo = QVBoxLayout(self.wiz_scroll)
        self.wiz_scroll_lo.setContentsMargins(0, 0, 0, 0)
        self.wiz_scroll_lo.setSpacing(4)

        lo.addWidget(self.wiz_scroll)
        br = QHBoxLayout()
        br.addWidget(self._btn("+ Add Machine", self._wiz_add))
        br.addWidget(self._btn("Save Config", self._wiz_save))
        lo.addLayout(br)
        lo.addStretch()
        self._tab("Init Wizard", lo)

    def _make_wiz_row(self, idx: int, is_first: bool = False) -> list[QWidget]:
        e1 = QLineEdit(f"node{idx}")
        e1.setMaximumWidth(90)
        e1.setToolTip("Logical name for this machine (e.g. node0, server1)")
        e2 = QLineEdit("localhost" if is_first else "")
        e2.setMaximumWidth(130)
        e2.setToolTip("IP address or hostname; use 'localhost' for this machine")
        cb = QComboBox()
        cb.addItems(["local", "ssh"])
        cb.setCurrentText("local" if is_first else "ssh")
        cb.setMaximumWidth(70)
        cb.setToolTip("'local' runs everything here; 'ssh' connects to a remote host. Only one local machine allowed.")
        # Disable 'local' option for non-first rows
        if not is_first:
            cb.model().item(0).setEnabled(False)
        s1 = QSpinBox(); s1.setRange(0, 10); s1.setValue(1 if idx == 0 else 0); s1.setMinimumWidth(100)
        s1.setToolTip("How many master nodes (metadata servers with Raft consensus) to run on this machine")
        s2 = QSpinBox(); s2.setRange(0, 10); s2.setValue(1 if idx == 0 else 0); s2.setMinimumWidth(100)
        s2.setToolTip("How many storage nodes (datanodes) to run on this machine")
        ch = QCheckBox(); ch.setChecked(idx == 0)
        ch.setToolTip("Enable the auditing stack (Elasticsearch + Grafana) on this machine")
        eu = QLineEdit()
        eu.setMaximumWidth(80)
        eu.setToolTip("SSH username (only needed for remote machines)")
        ep = QLineEdit(_fix_repo_root(UNIX_ROOT or str(REPO_ROOT))); ep.setMaximumWidth(160)
        ep.setToolTip("Absolute path to the DAFS repository on this machine")
        return [e1, e2, cb, s1, s2, ch, eu, ep]

    def _wiz_add(self):
        wg = self._make_wiz_row(len(self.wiz_widgets), len(self.wiz_widgets) == 0)

        # Store reference for removal
        row_layout = QHBoxLayout()
        row_layout.setSpacing(6)
        wg.append(row_layout)  # store layout at index 8

        headers = [
            ("Machine Name", ""),
            ("Host / IP", ""),
            ("Connection", "local=this PC, ssh=remote"),
            ("Masters", "# of Raft metadata servers"),
            ("Datanodes", "# of storage nodes"),
            ("Auditing", "Elasticsearch + Grafana"),
            ("SSH User", ""),
            ("Repo Path", ""),
        ]
        for i, (hdr, tip) in enumerate(headers):
            col = QVBoxLayout()
            col.setSpacing(1)
            lb = self._label(hdr, size=8, bold=True, color="#6c7086")
            if tip:
                lb.setToolTip(tip)
            col.addWidget(lb)
            col.addWidget(wg[i])
            row_layout.addLayout(col)

        # Remove button
        rm_btn = QPushButton("Remove")
        rm_btn.setFixedHeight(26)
        rm_btn.setStyleSheet("background: #f38ba8; color: #1e1e2e; font-size: 11px; font-weight: bold;")
        rm_btn.setToolTip("Remove this machine from the cluster")
        rm_btn.clicked.connect(lambda: self._wiz_remove(wg))

        col_rm = QVBoxLayout()
        col_rm.setSpacing(1)
        col_rm.addWidget(self._label("", size=8))
        col_rm.addWidget(rm_btn)
        row_layout.addLayout(col_rm)

        self.wiz_scroll_lo.addLayout(row_layout)
        self.wiz_widgets.append(wg)

    def _wiz_remove(self, wg: list):
        """Remove the wizard row containing these widgets."""
        if len(self.wiz_widgets) <= 1:
            return
        row_layout = wg[8] if len(wg) > 8 else None
        if row_layout:
            # Remove all child widgets from the layout
            while row_layout.count():
                item = row_layout.takeAt(0)
                if item and item.widget():
                    item.widget().deleteLater()
                elif item and item.layout():
                    while item.layout().count():
                        c = item.layout().takeAt(0)
                        if c and c.widget():
                            c.widget().deleteLater()
                    item.layout().deleteLater()
            row_layout.deleteLater()
        self.wiz_widgets.remove(wg)

    def _wiz_save(self):
        machines = []
        for wg in self.wiz_widgets:
            machines.append({
                "name": wg[0].text(), "host": wg[1].text(), "connection": wg[2].currentText(),
                "user": wg[6].text(), "repo_root": wg[7].text(),
                "master_count": wg[3].value(), "datanode_count": wg[4].value(),
                "enable_auditing": wg[5].isChecked(), "advertise_host": "",
            })
        if sum(m["master_count"] for m in machines) == 0:
            self._dash_info.setPlainText("At least one master required.")
            return
        # First machine must be local
        if machines[0]["connection"] != "local":
            self._dash_info.setPlainText("The first machine must be 'local'.")
            return
        bg(op_init, machines)
        self._dash_info.setPlainText("Saving configuration...")

    # ── Build ────────────────────────────────────────────────────────

    def _build_build(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Build Docker Images", bold=True, size=14))
        lo.addWidget(self._btn("Start Build", lambda: bg(op_build)))
        w = self._log_widget()
        self._register_log("BUILD", w)
        lo.addWidget(w, 1)
        self._tab("Build", lo)

    # ── Deploy ───────────────────────────────────────────────────────

    def _build_deploy(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Deploy Cluster (Ansible)", bold=True, size=14))
        lo.addWidget(self._label("Sudo password (optional, for logrotate):", size=9))
        self._become = QLineEdit()
        self._become.setEchoMode(QLineEdit.EchoMode.Password)
        self._become.setMaximumWidth(240)
        lo.addWidget(self._become)
        lo.addWidget(self._btn("Deploy", lambda: bg(op_deploy, self._become.text())))
        w = self._log_widget()
        self._register_log("DEPLOY", w)
        lo.addWidget(w, 1)
        self._tab("Deploy", lo)

    # ── Compose ──────────────────────────────────────────────────────

    def _build_compose(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Docker Compose", bold=True, size=14))
        br = QHBoxLayout()
        for text, act in [("Status", "ps"), ("Restart", "restart"), ("Stop", "stop"), ("Down", "down")]:
            br.addWidget(self._btn(text, lambda checked, a=act: bg(op_compose, a)))
        lo.addLayout(br)
        w = self._log_widget()
        self._register_log("COMPOSE", w)
        lo.addWidget(w, 1)
        self._tab("Compose", lo)

    # ── Logs ─────────────────────────────────────────────────────────

    def _build_logs(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Operation Logs", bold=True, size=14))
        lo.addWidget(self._btn("Clear", lambda: [b.clear() for b in self._log_boxes.values()]))
        w = self._log_widget()
        self._register_log("GLOBAL", w)
        lo.addWidget(w, 1)
        self._tab("Logs", lo)

    # ── Destroy ──────────────────────────────────────────────────────

    def _build_destroy(self):
        lo = QVBoxLayout()
        lo.setContentsMargins(16, 16, 16, 16)
        lo.setSpacing(8)
        lo.addWidget(self._label("Danger Zone", bold=True, size=14, color="#f38ba8"))
        lo.addWidget(self._label("Clean removes generated config. Destroy removes ALL containers, volumes, networks and data.", size=9))
        br = QHBoxLayout()
        br.addWidget(self._btn("Clean Config", lambda: bg(op_clean)))
        db = self._btn("Destroy Cluster", lambda: bg(op_destroy))
        db.setStyleSheet("background: #f38ba8; color: #1e1e2e;")
        br.addWidget(db)
        lo.addLayout(br)
        w = self._log_widget()
        self._register_log("DANGER", w)
        lo.addWidget(w, 1)
        self._tab("Destroy", lo)


# ── Entry ───────────────────────────────────────────────────────────────

def main():
    import sys
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()