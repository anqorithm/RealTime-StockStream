#!/bin/bash
set -e

# Function to check if Cassandra is up and running
function check_cassandra {
    while ! cqlsh -e "describe keyspaces" &>/dev/null; do
        echo "Waiting for Cassandra to be up..."
        sleep 10
    done
}

echo "Setting up Cassandra schema..."

# Wait for Cassandra to be ready
check_cassandra

# Execute schema setup commands
cqlsh -f /init-cassandra/init.cql -u cassandra -p cassandra

echo "Cassandra schema setup complete."
