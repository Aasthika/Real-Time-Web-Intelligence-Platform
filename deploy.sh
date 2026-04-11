#!/bin/bash

# A quick deployment script to push the Web Intelligence Platform to AWS EC2 Free Tier

if [ "$#" -ne 2 ]; then
    echo "Usage: ./deploy.sh <path-to-aws-key.pem> <ec2-user@ec2-ip-address>"
    echo "Example: ./deploy.sh ~/Downloads/wip-key.pem ubuntu@54.123.45.67"
    exit 1
fi

KEY_PATH=$1
SERVER=$2

echo ""
echo "🚀 Step 1: Connecting to EC2 to provision Swap Memory and install Docker..."
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "$SERVER" << 'REMOTE_SCRIPT'
    # 1. SETUP MASSIVE SWAP SPACE (FOR FREE TIER 1GB RAM LIMIT)
    if [ ! -f /swapfile ]; then
        echo "Creating 8GB Swap File to support heavy Java containers..."
        sudo fallocate -l 8G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=8192
        sudo chmod 600 /swapfile
        sudo mkswap /swapfile
        sudo swapon /swapfile
        echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    fi

    # 2. UPDATE AND INSTALL REQUIREMENTS
    sudo apt-get update -y
    sudo apt-get install -y docker.io docker-compose git rsync
    
    # 3. START DOCKER
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker $USER
    
    # 4. PREPARE DIR
    mkdir -p ~/web-intelligence-platform
REMOTE_SCRIPT

echo ""
echo "📦 Step 2: Syncing local project files to the EC2 server..."
rsync -avz -e "ssh -i $KEY_PATH -o StrictHostKeyChecking=no" \
    --exclude 'venv' --exclude '.git' --exclude '__pycache__' \
    --exclude 'data' --exclude 'spark-checkpoints' \
    ./ "$SERVER":~/web-intelligence-platform/

echo ""
echo "🏗️  Step 3: Building and launching the clustered containers remotely..."
ssh -i "$KEY_PATH" "$SERVER" << 'REMOTE_SCRIPT'
    cd ~/web-intelligence-platform
    
    sudo docker-compose down -v
    sudo docker-compose up -d --build
REMOTE_SCRIPT

echo ""
echo "✅ Deployment Complete! Your Real-Time Intelligence Platform is now booting in the cloud."
echo "CRITICAL: Because you are on a free tier swapping disk for RAM, it will take Cassandra and Spark about 8-10 minutes to finish initializing."
