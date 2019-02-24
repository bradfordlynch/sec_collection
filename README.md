# Collection of SEC Filings
Distributed collection of SEC filings using AWS SQS queues and S3 for storage. Enables collection of all 10-K, 10-Q, and 8-K filings in a few hours.
## Setup
Run `aws_setup.py` to configure remote queues on AWS.

## Worker Nodes
The worker nodes run the `collect_filings.py` script which pulls CIK's from the `to_collect` queue, looks up the desired filings in the specified input file, and then collects them if they haven't already been collected. This will result in an S3 bucket organized by CIK and containing the filings in a compressed zip archive as well as an index file listing the filings in the archive.