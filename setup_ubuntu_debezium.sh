#!/bin/bash
# This script downloads Debezium natively into your WSL environment (no Docker)

echo "Downloading Debezium MongoDB Connector v2.5.0..."
wget -q --show-progress https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/2.5.0.Final/debezium-connector-mongodb-2.5.0.Final-plugin.tar.gz

echo "Extracting plugin..."
mkdir -p ~/kafka-plugins/debezium
tar -xzf debezium-connector-mongodb-2.5.0.Final-plugin.tar.gz -C ~/kafka-plugins/debezium --strip-components=1

# Clean up
rm debezium-connector-mongodb-2.5.0.Final-plugin.tar.gz

echo "=========================================================="
echo "✅ Debezium installed successfully to ~/kafka-plugins/debezium!"
echo ""
echo "HOW TO START KAFKA CONNECT NATIVELY:"
echo "1. Go to your Apache Kafka installation folder in WSL."
echo "2. Edit the file: \`config/connect-distributed.properties\`"
echo "3. At the very bottom of that file, add this exact line:"
echo "   plugin.path=/home/$USER/kafka-plugins"
echo "4. Boot Kafka Connect with this command:"
echo "   ./bin/connect-distributed.sh config/connect-distributed.properties"
echo "=========================================================="
