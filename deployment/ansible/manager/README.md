# DAFS Cluster Manager (GUI)

Desktop GUI to bootstrap, deploy, and manage the DAFS cluster.
Built with [PySimpleGUI](https://pypi.org/project/PySimpleGUI/).

## Requirements

- Python ≥ 3.10
- Docker + Docker Compose plugin
- Ansible with `community.docker` collection:

  ```bash
  ansible-galaxy collection install community.docker
  ```

## Usage

```bash
cd deployment/ansible/manager
uv run python main.py
```

## Tabs

| Tab | Description |
|-----|-------------|
| 📊 Dashboard | Cluster overview (hosts, masters, datanodes, auditing) |
| 🧙 Init Wizard | Interactive cluster definition (machines, services) |
| 🔨 Build | Build Docker images locally |
| 🚀 Deploy | Run Ansible playbook to bring up the cluster |
| 📦 Compose | Docker Compose lifecycle (status, restart, stop, down) |
| 📋 Logs | Real-time operation logs (mirrors all tabs) |
| ⚠️ Destroy | Clean config or full cluster destruction |

## Generated artifacts

- `deployment/ansible/cluster_config.json`
- `deployment/ansible/generated/` (inventory + per-host vars)

Remote (SSH) builds are not supported from the GUI yet — use the CLI
`cluster_manager.py` for multi-machine deployments.