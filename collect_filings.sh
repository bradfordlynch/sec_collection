#!/usr/bin/env bash
rm -f collect_filings.py
rm -f util.py
rm -f sqs_util.py
rm -rf env
export PATH=~/.local/bin:$PATH

exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>~/output.log 2>&1
# Everything below will go to the file 'log.out':

# Wait a bit for network to come up
# echo "Copying app data from S3"
sleep 10

#============
# Setup environment
#============
# Copy app data from S3
echo "Copying app data from S3"
aws s3 cp s3://bll-sec-filings/app ~/ --recursive

chmod +x ~/collect_filings.sh

echo "Creating environment for app"
virtualenv env
source env/bin/activate

echo "Install app requirements"
pip3 install -r requirements.txt

#============
# Collect filings
#============
echo "Starting collection"
python3 ~/collect_filings.py

echo "Finished collection, copying log files to S3"
insId="`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`"
aws s3 cp ~/ s3://bll-sec-filings/logs/$insId/ --recursive --exclude "*" --include "*.log"
rm -f ~/*.log

echo "Waiting before shut down"
sleep 60

echo "Shutting down"
sudo /sbin/shutdown -h now
