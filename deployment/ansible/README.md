# DAFS Ansible Deploy (Compose)

This playbook is local-first and starts three compose stacks:

- `Auditing/docker-compose.yml`
- `deployment/docker-compose.master.yml` (3 masters: `master0`, `master1`, `master2`)
- `deployment/docker-compose.datanode.yml` (reused for one or many datanodes)

Note: `deployment/ansible/site.yml` no longer hardcodes a cluster topology.
You must provide `masters`/`datanodes` variables (recommended via
`cluster_manager.py`).

## Requirements

- Docker + Docker Compose plugin installed on target host(s)
- Source repository available on target host(s)
- Ansible collection `community.docker`

Install collection:

```bash
ansible-galaxy collection install community.docker
```

## Defaults

- Repo root: `/home/matzul/Documents/DAFS`
- Shared docker network: `dafs-net`
- `dafs_build_images: false` to avoid long blocking builds on each run

## Run (local)

```bash
ansible-playbook -i "localhost," -c local deployment/ansible/site.yml
```

## Build behavior

By default the playbook does `up -d` only.

Compose operations are now executed with `community.docker.docker_compose_v2`
for better idempotence.

Enable image builds explicitly when needed:

```bash
ansible-playbook -i "localhost," -c local deployment/ansible/site.yml \
  -e "dafs_build_images=true"
```

## Datanodes customization

Edit the `datanodes` list in `deployment/ansible/site.yml` to add more nodes. For each node:

- `compose_project`: unique compose project name
- `host_port`: unique published port
- `container_name`: unique container name
- `metadata_address`: leader hint endpoint (e.g. `http://master0:9090`)
- `metadata_endpoints`: comma-separated cluster endpoints

## Override example

```bash
ansible-playbook -i "localhost," -c local deployment/ansible/site.yml \
  -e "dafs_repo_root=/opt/DAFS" \
  -e "dafs_network_name=dafs-net"
```

## Cluster Manager (interactive)

Use `deployment/ansible/cluster_manager.py` to bootstrap a cluster from zero
and operate it later.

It lets you choose:

- number of machines
- connection mode per machine (`local` or `ssh`)
- which services run on each machine (auditing / masters / datanodes)

Then it supports lifecycle commands:

- `build`
- `up`
- `stop`
- `down`
- `restart`
- `status`
- `clean`

Examples:

```bash
python3 deployment/ansible/cluster_manager.py init
python3 deployment/ansible/cluster_manager.py show
python3 deployment/ansible/cluster_manager.py build
python3 deployment/ansible/cluster_manager.py up --verbose
python3 deployment/ansible/cluster_manager.py status
python3 deployment/ansible/cluster_manager.py stop
python3 deployment/ansible/cluster_manager.py down
python3 deployment/ansible/cluster_manager.py clean
```

Generated artifacts:

- `deployment/ansible/cluster_config.json`
- `deployment/ansible/generated/cluster_inventory.ini`
- `deployment/ansible/generated/vars-<host>.json`

## Log Rotation

The playbook also configures logrotate for Docker JSON logs:

- target: `/var/lib/docker/containers/*/*-json.log`
- config file: `/etc/logrotate.d/dafs-docker-json`
- defaults: daily, `rotate 14`, compression, `copytruncate`, `maxsize 100M`

This behavior can be toggled with `enable_logrotate` and tuned with
`logrotate_rotate_count` / `logrotate_max_size`.
