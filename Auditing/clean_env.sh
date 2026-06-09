cd Auditing
sudo docker compose down -v --remove-orphans
sudo find certs/ -mindepth 1 ! -name ".gitkeep" -exec rm -rf {} +
sudo find esdata01/ -mindepth 1 ! -name ".gitkeep" -exec rm -rf {} +
sudo find grafana/storage/ -mindepth 1 ! -name ".gitkeep" -exec rm -rf {} +