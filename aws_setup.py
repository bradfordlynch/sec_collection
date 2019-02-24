import argparse

import boto3

from sqs_util import sqs, _build_dead_letter_queue

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--projName', default='sec_filings_')
  parser.add_argument('--setup')
  parser.add_argument('--teardown')
  args = parser.parse_args()

  qName = 'ciks_to_collect'

  if args.setup:
    assert not args.teardown, 'Arguments setup and teardown cannot both be specified'
    print('Creating dead letter queue')
    attrs = _build_dead_letter_queue(args.projName, qName)
    
    print('Creating main queue')
    res = sqs.create_queue(
      QueueName=args.projName + qName,
      Attributes=attrs
    )
  if args.teardown:
    assert not args.setup, ''

    print('Deleting main queue')
    qUri = sqs.get_queue_url(QueueName=args.projName + qName)['QueueUrl']
    res = sqs.delete_queue(QueueUrl=qUri)

    print('Deleting dead letter queue')
    qUri = sqs.get_queue_url(QueueName=args.projName + qName + '_DeadLetter')['QueueUrl']
    res = sqs.delete_queue(QueueUrl=qUri)