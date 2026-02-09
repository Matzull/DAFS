#!/bin/bash

# Elasticsearch Setup Script
# This script automates the setup of Elasticsearch with custom users and roles

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

echo "========================================="
echo "  Elasticsearch Setup Script"
echo "========================================="
echo ""

# Ask for credentials
read -p "Enter admin username (default: myadmin): " ADMIN_USER
ADMIN_USER=${ADMIN_USER:-myadmin}

read -sp "Enter admin password: " ADMIN_PASSWORD
echo ""

if [ -z "$ADMIN_PASSWORD" ]; then
    echo "Error: Password cannot be empty"
    exit 1
fi

read -sp "Confirm admin password: " ADMIN_PASSWORD_CONFIRM
echo ""

if [ "$ADMIN_PASSWORD" != "$ADMIN_PASSWORD_CONFIRM" ]; then
    echo "Error: Passwords do not match"
    exit 1
fi

# Ask for elastic superuser password (used for initial setup)
read -sp "Enter elastic superuser password (for initial setup): " ELASTIC_PASSWORD
echo ""

if [ -z "$ELASTIC_PASSWORD" ]; then
    echo "Error: Elastic password cannot be empty"
    exit 1
fi

# Create .env file
echo "Creating .env file..."
cat > "$ENV_FILE" << EOF
# Elasticsearch credentials
ELASTIC_PASSWORD=$ELASTIC_PASSWORD
ADMIN_USER=$ADMIN_USER
ADMIN_PASSWORD=$ADMIN_PASSWORD
EOF

echo ".env file created at $ENV_FILE"
echo ""

# Start Elasticsearch container
echo "Starting Elasticsearch container..."
cd "$SCRIPT_DIR"
docker compose up -d elasticsearch

# Wait for Elasticsearch to be ready
echo ""
echo "Waiting for Elasticsearch to be ready..."
echo "This may take a minute or two..."

MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s -u "elastic:$ELASTIC_PASSWORD" http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        echo "Elasticsearch is ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo ""
    echo "Error: Elasticsearch did not become ready in time"
    echo "Check logs with: docker logs -f elasticsearch"
    exit 1
fi

echo ""

# Create custom role
echo "Creating kibana_system_access role..."
ROLE_RESPONSE=$(curl -s -X POST "http://localhost:9200/_security/role/kibana_system_access" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "cluster": ["all"],
    "indices": [
      {
        "names": [".kibana*", ".apm*"],
        "privileges": ["all"],
        "allow_restricted_indices": true
      }
    ]
  }')

if echo "$ROLE_RESPONSE" | grep -q '"created":true'; then
    echo "✓ Role 'kibana_system_access' created successfully"
else
    echo "Role creation response: $ROLE_RESPONSE"
fi

# Create custom user
echo "Creating user '$ADMIN_USER'..."
USER_RESPONSE=$(curl -s -X POST "http://localhost:9200/_security/user/$ADMIN_USER" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d "{
    \"password\": \"$ADMIN_PASSWORD\",
    \"roles\": [\"superuser\", \"kibana_system_access\"],
    \"full_name\": \"Lab Admin\"
  }")

if echo "$USER_RESPONSE" | grep -q '"created":true'; then
    echo "✓ User '$ADMIN_USER' created successfully"
else
    echo "User creation response: $USER_RESPONSE"
fi

echo ""

# Start the full stack
echo "Starting full stack..."
docker compose up -d

echo ""
echo "========================================="
echo "  Setup Complete!"
echo "========================================="
echo ""
echo "Access Kibana at: http://localhost:5601"
echo ""
echo "Credentials:"
echo "  Username: $ADMIN_USER"
echo "  Password: (as configured)"
echo ""
echo "To view logs: docker compose logs -f"
echo "To stop: docker compose down"
echo ""
