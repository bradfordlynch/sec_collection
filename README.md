# Collection of SEC Filings
## Setup
Run `aws_setup.py` to configure remote queues on AWS.

## Worker Nodes
The worker nodes run the `collect_filings.py` script which pulls CIK's from the `to_collect` queue, looks up the desired filings in the specified input file, and then collects them if they haven't already been collected. This will result in an S3 bucket organized by CIK and containing the filings in a compressed zip archive as well as an index file listing the filings in the archive.