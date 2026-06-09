#!/usr/bin/env python3
"""Interactive DAFS cluster bootstrap and lifecycle manager.

This tool creates a cluster definition (hosts + service placement) and then
executes build/up/stop/down/status operations through Ansible.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PLAYBOOK_PATH = REPO_ROOT / "deployment" / "ansible" / "site.yml"
GENERATED_DIR = REPO_ROOT / "deployment" / "ansible" / "generated"
CONFIG_PATH = REPO_ROOT / "deployment" / "ansible" / "cluster_config.json"
INVENTORY_PATH = GENERATED_DIR / "cluster_inventory.ini"
DESTROY_CONFIRMATION_PHRASE = "I understand the consequences"


def run_cmd(cmd: list[str]) -> None:
    print("\n$", " ".join(cmd))
    subprocess.run(cmd, check=True)


def run_cmd_capture(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    print("\n$", " ".join(cmd))
    return subprocess.run(cmd, check=True, text=True, capture_output=True)


def ask(prompt: str, default: str | None = None) -> str:
    if default is None:
        value = input(f"{prompt}: ").strip()
    else:
        value = input(f"{prompt} [{default}]: ").strip()
    if value:
        return value
    return default or ""


def ask_int(prompt: str, default: int, minimum: int = 0) -> int:
    while True:
        raw = ask(prompt, str(default))
        try:
            value = int(raw)
            if value < minimum:
                raise ValueError
            return value
        except ValueError:
            print(f"Enter an integer >= {minimum}.")


def ask_bool(prompt: str, default: bool) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        raw = input(f"{prompt} [{hint}]: ").strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes", "1", "true"}:
            return True
        if raw in {"n", "no", "0", "false"}:
            return False
        print("Answer y or n.")


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Cluster config not found at {path}. Run `init` first."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def inventory_line(host: dict[str, Any]) -> str:
    name = host["name"]
    connection = host["connection"]
    if connection == "local":
        return f"{name} ansible_connection=local ansible_python_interpreter=auto_silent"

    host_ip = host["host"]
    user = host.get("user")
    if user:
        return f"{name} ansible_host={host_ip} ansible_user={user} ansible_connection=ssh"
    return f"{name} ansible_host={host_ip} ansible_connection=ssh"


def write_generated_files(config: dict[str, Any]) -> dict[str, Path]:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    inventory_lines = ["[cluster]"]
    vars_files: dict[str, Path] = {}

    for host in config["hosts"]:
        inventory_lines.append(inventory_line(host))
        vars_obj = {
            "dafs_repo_root": host["repo_root"],
            "dafs_network_name": config["dafs_network_name"],
            "auditing_network_name": config["auditing_network_name"],
            "enable_auditing": host["enable_auditing"],
            "enable_masters": bool(host["masters"]),
            "enable_datanodes": bool(host["datanodes"]),
            "master_cluster_members": config["master_cluster_members"],
            "masters": host["masters"],
            "datanodes": host["datanodes"],
        }
        vars_path = GENERATED_DIR / f"vars-{host['name']}.json"
        save_json(vars_path, vars_obj)
        vars_files[host["name"]] = vars_path

    INVENTORY_PATH.write_text("\n".join(inventory_lines) + "\n", encoding="utf-8")
    return vars_files


def shell_on_host(host: str, script: str) -> None:
    run_cmd(
        [
            "ansible",
            host,
            "-i",
            str(INVENTORY_PATH),
            "-m",
            "ansible.builtin.shell",
            "-a",
            script,
        ]
    )


def shell_on_host_with_extra(
    host: str, script: str, extra_vars: dict[str, Any] | None = None
) -> None:
    cmd = [
        "ansible",
        host,
        "-i",
        str(INVENTORY_PATH),
        "-m",
        "ansible.builtin.shell",
        "-a",
        script,
    ]
    if extra_vars:
        cmd.extend(["-e", json.dumps(extra_vars)])
    run_cmd(cmd)


def run_ssh_command(host_cfg: dict[str, Any], command: str) -> None:
    host = host_cfg["host"]
    user = host_cfg.get("user")
    target = f"{user}@{host}" if user else str(host)
    run_cmd(["ssh", target, "bash", "-lc", command])


def host_compose_commands(host_cfg: dict[str, Any], action: str) -> list[str]:
    repo = host_cfg["repo_root"]
    cmds: list[str] = []

    if host_cfg["enable_auditing"]:
        cmds.append(
            f"docker compose -p dafs-auditing -f {repo}/Auditing/docker-compose.yml {action}"
        )

    for master in host_cfg["masters"]:
        cmds.append(
            "docker compose "
            f"-p {master['compose_project']} "
            f"-f {repo}/deployment/docker-compose.master.yml {action}"
        )

    for datanode in host_cfg["datanodes"]:
        cmds.append(
            "docker compose "
            f"-p {datanode['compose_project']} "
            f"-f {repo}/deployment/docker-compose.datanode.yml {action}"
        )

    return cmds


def host_build_commands(host_cfg: dict[str, Any]) -> list[str]:
    repo = host_cfg["repo_root"]
    cmds: list[str] = []
    build_prefix = "DOCKER_BUILDKIT=1 BUILDKIT_PROGRESS=plain "

    if host_cfg["enable_auditing"]:
        cmds.append(
            f"{build_prefix}docker compose -p dafs-auditing -f {repo}/Auditing/docker-compose.yml build --progress=plain"
        )

    if host_cfg["masters"]:
        cmds.append(
            f"{build_prefix}docker compose -f {repo}/deployment/docker-compose.master.yml build --progress=plain"
        )

    if host_cfg["datanodes"]:
        cmds.append(
            f"{build_prefix}docker compose -f {repo}/deployment/docker-compose.datanode.yml build --progress=plain"
        )

    return cmds


def cmd_init(args: argparse.Namespace) -> None:
    print("Cluster initialization wizard")
    host_count = ask_int("How many machines?", 1, minimum=1)

    machine_inputs: list[dict[str, Any]] = []
    for idx in range(host_count):
        print(f"\nMachine #{idx + 1}")
        default_name = f"node{idx}"
        name = ask("Machine logical name", default_name)
        host = ask("Ansible host (hostname/IP or localhost)", "localhost")
        default_conn = "local" if host in {"localhost", "127.0.0.1"} else "ssh"
        connection = ask("Connection type (local/ssh)", default_conn).lower()
        if connection not in {"local", "ssh"}:
            raise ValueError("Connection type must be local or ssh")
        user = ask("SSH user (empty for default)", "") if connection == "ssh" else ""
        repo_root = ask("Repository path on this machine", str(REPO_ROOT))
        advertise_host = ask(
            "Advertise host for cross-node endpoints",
            host if connection == "ssh" else "",
        )
        enable_auditing = ask_bool("Run auditing stack on this machine?", idx == 0)
        master_count = ask_int("How many masters on this machine?", 1 if idx == 0 else 0)
        datanode_count = ask_int("How many datanodes on this machine?", 1 if idx == 0 else 0)

        machine_inputs.append(
            {
                "name": name,
                "host": host,
                "connection": connection,
                "user": user or None,
                "repo_root": repo_root,
                "advertise_host": advertise_host,
                "enable_auditing": enable_auditing,
                "master_count": master_count,
                "datanode_count": datanode_count,
            }
        )

    total_masters = sum(m["master_count"] for m in machine_inputs)
    if total_masters == 0:
        raise ValueError("At least one master is required.")

    hosts_cfg: list[dict[str, Any]] = []
    master_cluster_members: list[dict[str, Any]] = []

    master_id = 0
    datanode_id = 0

    for machine in machine_inputs:
        masters: list[dict[str, Any]] = []
        datanodes: list[dict[str, Any]] = []

        for local_idx in range(machine["master_count"]):
            node_id = master_id
            container_name = f"master{node_id}"
            service_port = 9090 + node_id
            config_candidate = REPO_ROOT / "services" / "master" / f"config-node{node_id}.toml"
            config_path = (
                f"../services/master/config-node{node_id}.toml"
                if config_candidate.exists()
                else "../services/master/config.toml"
            )

            if machine["connection"] == "local":
                endpoint_host = container_name
            else:
                endpoint_host = machine["advertise_host"] or machine["host"]

            master = {
                "compose_project": f"dafs-master-{node_id}",
                "container_name": container_name,
                "node_id": node_id,
                "raft_bootstrap": node_id == 0,
                "config_path": config_path,
                "host_port": service_port,
                "service_port": service_port,
                "listen_address": f"[::]:{service_port}",
                "wal_path": f"/app/data/master{node_id}.wal",
                "auth_db_path": f"/app/data/users{node_id}.db",
                "metadata_host": endpoint_host,
                "raft_host": endpoint_host,
            }
            masters.append(master)
            master_cluster_members.append(master.copy())
            master_id += 1

        for local_idx in range(machine["datanode_count"]):
            host_port = 8080 + local_idx
            datanode = {
                "compose_project": f"dafs-datanode-{datanode_id}",
                "config_path": "../services/datanode/config.toml",
                "host_port": str(host_port),
                "container_name": f"dafs-datanode-{datanode_id}",
                "rust_log": "info",
                "listen_address": "[::]:8080",
            }
            datanodes.append(datanode)
            datanode_id += 1

        hosts_cfg.append(
            {
                "name": machine["name"],
                "host": machine["host"],
                "connection": machine["connection"],
                "user": machine["user"],
                "repo_root": machine["repo_root"],
                "enable_auditing": machine["enable_auditing"],
                "masters": masters,
                "datanodes": datanodes,
            }
        )

    config = {
        "version": 1,
        "dafs_network_name": "dafs-net",
        "auditing_network_name": "elastic",
        "master_cluster_members": master_cluster_members,
        "hosts": hosts_cfg,
    }

    save_json(CONFIG_PATH, config)
    write_generated_files(config)

    print("\nCluster config written:")
    print(f"- {CONFIG_PATH}")
    print(f"- {INVENTORY_PATH}")
    print("- deployment/ansible/generated/vars-<host>.json")


def cmd_show(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    write_generated_files(config)

    print(f"Config: {CONFIG_PATH}")
    print(f"Inventory: {INVENTORY_PATH}")
    print("\nHosts:")
    for host in config["hosts"]:
        print(
            f"- {host['name']} ({host['connection']}:{host['host']}), "
            f"auditing={host['enable_auditing']}, "
            f"masters={len(host['masters'])}, datanodes={len(host['datanodes'])}"
        )


def cmd_up(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    vars_files = write_generated_files(config)
    if args.build:
        print("\nRunning pre-build phase before ansible up (streaming local build output when possible)...")
        cmd_build(args)

    become_password: str | None = os.environ.get("ANSIBLE_BECOME_PASSWORD")
    if (args.become_pass or args.ask_become_pass) and become_password is None:
        become_password = getpass.getpass("Sudo password: ")

    if args.logrotate and (become_password is None):
        raise RuntimeError(
            "Logrotate is enabled but no become auth method was provided. "
            "Use --become-pass (or --ask-become-pass), or set ANSIBLE_BECOME_PASSWORD. "
            "Alternatively disable it for this run with --no-logrotate."
        )

    for host in config["hosts"]:
        host_name = host["name"]
        cmd = [
            "ansible-playbook",
            "-vvvvvv" if args.verbose else "-vv",
            "-i",
            str(INVENTORY_PATH),
            "-l",
            host_name,
            str(PLAYBOOK_PATH),
            "-e",
            f"@{vars_files[host_name]}",
            "-e",
            "dafs_build_images=false",
            "-e",
            f"enable_logrotate={'true' if args.logrotate else 'false'}",
        ]
        if become_password is not None:
            cmd.extend(
                [
                    "-e",
                    json.dumps(
                        {
                            "ansible_become_password": become_password,
                            "logrotate_sudo_password": become_password,
                        }
                    ),
                ]
            )
        run_cmd(cmd)


def cmd_build(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    write_generated_files(config)

    for host in config["hosts"]:
        build_cmds = host_build_commands(host)
        if not build_cmds:
            continue

        if host["connection"] == "local":
            print(f"\n===== {host['name']} (local direct build) =====")
            for build_cmd in build_cmds:
                run_cmd(["bash", "-lc", build_cmd])
            continue

        if host["connection"] == "ssh" and host.get("host"):
            print(f"\n===== {host['name']} (remote ssh direct build) =====")
            for build_cmd in build_cmds:
                run_ssh_command(host, build_cmd)
            continue

        print(f"\n===== {host['name']} (remote ansible fallback build) =====")
        for build_cmd in build_cmds:
            shell_on_host(host["name"], build_cmd)


def cmd_compose_action(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    write_generated_files(config)

    for host in config["hosts"]:
        commands = host_compose_commands(host, args.action)
        if not commands:
            continue
        script = " && ".join(commands)
        shell_on_host(host["name"], script)


def cmd_status(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    write_generated_files(config)

    for host in config["hosts"]:
        print(f"\n===== {host['name']} =====")
        for status_cmd in host_compose_commands(host, "ps"):
            shell_on_host(host["name"], status_cmd)


def cmd_clean(args: argparse.Namespace) -> None:
    targets = [CONFIG_PATH, GENERATED_DIR]
    existing = [p for p in targets if p.exists()]
    if not existing:
        print("Nothing to clean.")
        return

    if not args.yes:
        print("This will remove generated cluster definition files:")
        for path in existing:
            print(f"- {path}")
        if not ask_bool("Continue?", False):
            print("Aborted.")
            return

    if CONFIG_PATH.exists():
        CONFIG_PATH.unlink()
    if GENERATED_DIR.exists():
        shutil.rmtree(GENERATED_DIR)

    print("Removed cluster config and generated inventory/vars files.")


def build_destroy_script(host_cfg: dict[str, Any], become_password: str | None) -> str:
    repo = shlex.quote(host_cfg["repo_root"])
    commands: list[str] = ["set +e"]

    # Stop/remove stacks and their named volumes/images.
    if host_cfg["enable_auditing"]:
        commands.append(
            f"docker compose -p dafs-auditing -f {repo}/Auditing/docker-compose.yml down --volumes --remove-orphans --rmi local || true"
        )

    for master in host_cfg["masters"]:
        project = shlex.quote(master["compose_project"])
        commands.append(
            f"docker compose -p {project} -f {repo}/deployment/docker-compose.master.yml down --volumes --remove-orphans --rmi local || true"
        )

    for datanode in host_cfg["datanodes"]:
        project = shlex.quote(datanode["compose_project"])
        commands.append(
            f"docker compose -p {project} -f {repo}/deployment/docker-compose.datanode.yml down --volumes --remove-orphans --rmi local || true"
        )

    # Remove fixed-name auditing containers if they still exist from prior runs.
    commands.append("docker rm -f grafana ai-assistant-backend >/dev/null 2>&1 || true")

    # Remove externally managed shared networks if possible.
    commands.append("docker network rm dafs-net >/dev/null 2>&1 || true")
    commands.append("docker network rm elastic >/dev/null 2>&1 || true")

    # Remove bind-mounted mutable auditing data from repository path.
    commands.append(f"rm -rf {repo}/Auditing/certs")
    commands.append(f"rm -rf {repo}/Auditing/esdata")
    commands.append(f"rm -rf {repo}/Auditing/esdata01")
    commands.append(f"rm -rf {repo}/Auditing/grafana/storage")

    # Remove logrotate policy and temporary policy file.
    if become_password:
        pw = shlex.quote(become_password)
        commands.append(
            f"printf '%s\\n' {pw} | sudo -S -p '' rm -f /etc/logrotate.d/dafs-docker-json || true"
        )
    else:
        commands.append("sudo rm -f /etc/logrotate.d/dafs-docker-json >/dev/null 2>&1 || true")
    commands.append("rm -f /tmp/dafs-docker-json.logrotate || true")

    return "\n".join(commands)


def cmd_destroy(args: argparse.Namespace) -> None:
    config = load_config(CONFIG_PATH)
    write_generated_files(config)

    print("\nWARNING: This will permanently destroy the cluster resources.")
    print("- Containers, images and volumes created by compose")
    print("- External docker networks (dafs-net, elastic) when removable")
    print("- Auditing mutable mounted data under the repository")
    print("- Logrotate policy at /etc/logrotate.d/dafs-docker-json")

    typed = input(
        f'If you want to permanently destroy the cluster please write "{DESTROY_CONFIRMATION_PHRASE}": '
    ).strip()
    if typed != DESTROY_CONFIRMATION_PHRASE:
        raise RuntimeError("Destroy aborted: confirmation phrase did not match exactly.")

    become_password: str | None = os.environ.get("ANSIBLE_BECOME_PASSWORD")
    if become_password is None:
        typed_pw = getpass.getpass(
            "Sudo password for removing /etc/logrotate.d/dafs-docker-json (leave empty to try passwordless sudo): "
        )
        become_password = typed_pw or None

    for host in config["hosts"]:
        script = build_destroy_script(host, become_password)
        shell_on_host_with_extra(host["name"], script)

    if CONFIG_PATH.exists():
        CONFIG_PATH.unlink()
    if GENERATED_DIR.exists():
        shutil.rmtree(GENERATED_DIR)

    print("Destroy complete: cluster resources and generated config files removed.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DAFS cluster bootstrap and lifecycle manager")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Interactive cluster initialization")
    sub.add_parser("show", help="Show current cluster definition")

    up = sub.add_parser("up", help="Bring cluster up with ansible playbook")
    up.add_argument("--build", action="store_true", help="Build images while bringing up")
    up.add_argument("--verbose", action="store_true", help="Use -vvvvvv on ansible-playbook")
    up.add_argument(
        "--ask-become-pass",
        action="store_true",
        help="Prompt for sudo/become password (required when logrotate tasks need privileges)",
    )
    up.add_argument(
        "--become-pass",
        action="store_true",
        help="Prompt once in cluster_manager and pass ansible_become_password as JSON extra-vars",
    )
    up.add_argument(
        "--no-logrotate",
        dest="logrotate",
        action="store_false",
        help="Skip logrotate tasks for this run",
    )
    up.set_defaults(logrotate=True)

    sub.add_parser("build", help="Build compose images on configured hosts (plain BuildKit output)")

    stop = sub.add_parser("stop", help="Stop cluster containers")
    stop.set_defaults(action="stop")

    down = sub.add_parser("down", help="Stop and remove cluster containers")
    down.set_defaults(action="down")

    restart = sub.add_parser("restart", help="Restart cluster containers")
    restart.set_defaults(action="restart")

    sub.add_parser("status", help="Show cluster compose status")

    clean = sub.add_parser("clean", help="Delete generated cluster config and inventory/vars")
    clean.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

    sub.add_parser(
        "destroy",
        help=(
            "Permanently destroy cluster resources (containers/images/volumes/networks), "
            "remove logrotate policy and delete generated config"
        ),
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "init":
            cmd_init(args)
        elif args.command == "show":
            cmd_show(args)
        elif args.command == "up":
            cmd_up(args)
        elif args.command == "build":
            cmd_build(args)
        elif args.command in {"stop", "down", "restart"}:
            cmd_compose_action(args)
        elif args.command == "status":
            cmd_status(args)
        elif args.command == "clean":
            cmd_clean(args)
        elif args.command == "destroy":
            cmd_destroy(args)
        else:
            parser.error("Unknown command")
    except subprocess.CalledProcessError as exc:
        print(f"\nCommand failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
