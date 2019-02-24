import argparse
from datetime import datetime
import logging
import os
import random
import time

import pandas as pd

from util import _mirror_s3, _update_s3, _cleanup, _maybe_download_filing, _handle_unexpected_error, _update_index
from sqs_util import sqs, _receive_messages, _delete_message

DT_FMT = "%Y%m%d-%H%M%S"

logging.basicConfig( \
  filename='sec_filings_{}.log'.format(datetime.now().strftime(DT_FMT)), \
  format='%(asctime)s - %(levelname)s - %(message)s', \
  level=logging.INFO
)

logger = logging.getLogger(__name__)

S3_BUCKET = 'bll-sec-filings'
CIK_QUEUE = 'ciks_to_collect'

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--projName', default='sec_filings_')
  parser.add_argument('--filings', default='all_10K_10Q.zip')
  args = parser.parse_args()

  filings = pd.read_csv(args.filings)
  filings = filings.set_index('cik')
  qUri = sqs.get_queue_url(QueueName=args.projName + CIK_QUEUE)['QueueUrl']

  while True:
    msgs = _receive_messages(qUri, 1)

    if len(msgs) > 0:
      for msg in msgs:
        # Get the filenames of the firm's filings 
        cikFilings = filings.loc[int(msg['cik'])]
        try:
          cikPath = os.path.join(*cikFilings.iloc[0]['SECFNAME'].split('/')[:-1])
        except TypeError:
          cikFilings = filings.reset_index()
          cikFilings = cikFilings.loc[cikFilings.cik == int(msg['cik'])].set_index('cik')
          cikPath = os.path.join(*cikFilings.iloc[0]['SECFNAME'].split('/')[:-1])
        except Exception as e:
          _handle_unexpected_error(e, 'Unexpected error when building CIK path')

        if cikPath:
          # Mirror S3
          _mirror_s3(cikPath, S3_BUCKET)

          changed = False

          # Maybe collect filings from SEC
          for fn in cikFilings.SECFNAME.values:
            dlStart = time.time()
            numBytes = _maybe_download_filing(fn)
            dlEnd = time.time()

            if numBytes:
              changed = True
              # Wait some time before making another request
              time.sleep(max(random.uniform(2,5)*(dlEnd - dlStart), 0.75))

          # Update S3 with firm's filings
          _update_index(cikPath)
          if changed:
            _update_s3(cikPath, S3_BUCKET)
          else:
            logger.info('Did not download anything new, only pushing index to S3')
            _update_s3(cikPath, S3_BUCKET, filings=False)

          # Remove local copy of files
          _cleanup(cikPath)

          # Delete message from queue
          try:
            _delete_message(msg)
          except Exception as e:
            logger.error('Unexpected error when deleting message')
            logger.error(type(e))
            logger.error(e)
        else:
          pass
    else:
      logger.info('Nothing to collect, sleeping')
      time.sleep(60)