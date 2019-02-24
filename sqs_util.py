import json
import logging

import boto3

logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name='us-east-1')

def _build_dead_letter_queue(prefix, queueName):
  res = sqs.create_queue(
    QueueName=prefix + queueName + '_DeadLetter',
    Attributes={
      'MessageRetentionPeriod': '1209600',
      'VisibilityTimeout': '600'
    }
  )

  res = sqs.get_queue_attributes(
    QueueUrl=res['QueueUrl'],
    AttributeNames=['QueueArn']
  )

  deadLetterArn = res['Attributes']['QueueArn']

  redrive = {
    'deadLetterTargetArn': deadLetterArn,
    'maxReceiveCount': 3
  }

  attrs = {
      'MessageRetentionPeriod': '1209600',
      'VisibilityTimeout': '600',
      'RedrivePolicy': json.dumps(redrive)
  }

  return attrs

def _send_message(msg, destQueueUri):
  body = {}

  for key in msg:
    if not key in ['ReceiptHandle', 'MessageId', 'queueUri']:
      body[key] = msg[key]

  res = sqs.send_message(
    QueueUrl=destQueueUri,
    MessageBody=json.dumps(body)
  )

  logger.info('Sent message to {}'.format(destQueueUri))

  return res

def _receive_messages(queueUri, maxNumMessages=10):
  res = sqs.receive_message(
    QueueUrl=queueUri,
    MaxNumberOfMessages=maxNumMessages
  )

  msgs = []

  if 'Messages' in res:
    for raw_msg in res['Messages']:
      msg = json.loads(raw_msg['Body'])
      msg['ReceiptHandle'] = raw_msg['ReceiptHandle']
      msg['MessageId'] = raw_msg['MessageId']
      msg['queueUri'] = queueUri

      msgs.append(msg)

  logger.info('Received {} messages from {} queue'.format(len(msgs), queueUri))

  return msgs

def _delete_message(msg):
  res = sqs.delete_message(
    QueueUrl=msg['queueUri'],
    ReceiptHandle=msg['ReceiptHandle']
  )

  logger.info('Deleted {} from queue {}'.format(msg['MessageId'], msg['queueUri']))

  return res