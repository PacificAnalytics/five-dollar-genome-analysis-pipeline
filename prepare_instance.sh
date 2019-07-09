#! /usr/bin/env bash

# This is the script for the automated setup of Google instances
# for running Cromwell tasks

# Variables for all (copy with snippets)
MOUNT_DIR="/home/oggy/big"
DOCKER_DIR="/home/oggy/big/Docker"
CROMWELL_DIR="/home/oggy/big"

# Format and attach disk if needed
sudo mkdir -p $MOUNT_DIR
DEV_ID=sdb
sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/$DEV_ID
sudo mount -o discard,defaults /dev/$DEV_ID $MOUNT_DIR
sudo chmod a+w /dev/$DEV_ID $MOUNT_DIR

# Install Gsutil
sudo apt -y install expect
GINSTALL=$(expect -c "
set timeout 20
spawn sh -c {curl https://sdk.cloud.google.com | bash}
expect \"Installation directory (this will create a google-cloud-sdk subdirectory) (/home/oggy):\"
send \"\r\"
expect \"Remove it before installing? (y/N):\"
send \"y\r\"
expect \"Do you want to help improve the Google Cloud SDK (Y/n)? \"
send \"n\r\"
expect \"continue\"
send \"y\r\"
expect \"bashrc\"
send \"\r\"
expect eof
")
echo "$GINSTALL"

# Install docker and change default directory
sudo apt update
sudo apt -y install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt update
sudo apt -y install docker-ce
sudo sh -c 'echo {\"graph\": \"'$DOCKER_DIR'\"} > /etc/docker/daemon.json'
sudo mkdir $DOCKER_DIR
sudo service docker restart

# Create a swap file
sudo fallocate -l 1G $MOUNT_DIR/swapfile  # create file
sudo chmod 600 $MOUNT_DIR/swapfile
sudo mkswap $MOUNT_DIR/swapfile
sudo swapon $MOUNT_DIR/swapfile

# Get Cromwell and dependencies
sudo apt -y install openjdk-8-jre-headless
sudo mkdir $CROMWELL_DIR
cd $CROMWELL_DIR
sudo wget https://github.com/broadinstitute/cromwell/releases/download/42/cromwell-42.jar

# Install MySQL
#sudo apt -y install mysql-server expect
export DEBIAN_FRONTEND="noninteractive"
rootpw="test123"
sudo debconf-set-selections <<< "mysql-community-server mysql-community-server/root-pass password $rootpw"
sudo debconf-set-selections <<< "mysql-community-server mysql-community-server/re-root-pass password $rootpw"

sudo apt -y install dirmngr
sudo apt-key adv --keyserver pgp.mit.edu --recv-keys 5072E1F5
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv-keys 5072E1F5
cat <<- EOF > /etc/apt/sources.list.d/mysql.list
deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7
EOF
sudo apt-get update
sudo apt -y install mysql-community-server


SECURE_MYSQL=$(expect -c "
set timeout 10
spawn mysql_secure_installation
expect \"Enter password for user root:\"
send \"$rootpw\r\"
expect \"Press y|Y for Yes, any other key for No:\"
send \"n\r\"
expect \"Change the password for root ?\"
send \"n\r\"
expect \"Remove anonymous users?\"
send \"n\r\"
expect \"Disallow root login remotely?\"
send \"y\r\"
expect \"Remove test database and access to it?\"
send \"n\r\"
expect \"Reload privilege tables now?\"
send \"y\r\"
expect eof
")

echo "$SECURE_MYSQL"

# Configure MySQL new user and db
DATABASE="cromwell"
USER="ogtest"
PASS="12345678"
MYSQL=`which mysql`
Q1="CREATE DATABASE IF NOT EXISTS $DATABASE;"
Q2="GRANT ALL ON *.* TO '$USER'@'localhost' IDENTIFIED BY '$PASS';"
Q3="FLUSH PRIVILEGES;"
SQL="${Q1}${Q2}${Q3}"
sudo $MYSQL -u root --password="$rootpw" -e "$SQL"


echo "Done! "
echo "Restart shell with exec -l $SHELL"
echo "Run google init to login."
