import zipfile
import os
import logging
import time
import urllib
import urllib.request
import random

import boto3
from botocore.exceptions import ClientError
import pandas as pd

logger = logging.getLogger(__name__)

ROOT_DIR = '/tmp/sec'

s3_client = boto3.client('s3')

def _handle_unexpected_error(e, msg=None):
  if msg:
    logger.error(msg)
  logger.error('Unexpected error occured: {}'.format(type(e)))
  logger.error(e)

def _remove_bad_files_from_arcive(path):
  archive = zipfile.ZipFile(path, 'r')
  success = False

  if len(archive.namelist()) > len(set(archive.namelist())):
    logger.error('Archive has duplicate files, removing files with null size')
    try:
      newPath = path.split('.')[0] + '_new' + path.split('.')[1]
      with zipfile.ZipFile(newPath, 'w', compression=zipfile.ZIP_DEFLATED) as newArchive:
        for f in archive.infolist():
          if f.file_size > 0:
            logger.info('Valid file: {}'.format(f.filename))
            with newArchive.open(f.filename, 'w') as validFile:
              validFile.write(archive.open(f).read())
          else:
            logger.error('Zero size: {}'.format(f.filename))

      success = True
    except Exception as e:
      logger.info('Unexpected error while cleaning archive')
      raise e
    finally:
      archive.close()

      if success:
        # Remove bad archive
        logger.info('Removing bad archive')
        os.remove(path)

        # Rename new archive
        logger.info('Renaming new archive')
        os.rename(newPath, path)
      else:
        logger.info('Failed to clean archive')
  else:
    logger.info('Archive has no duplicate files')

  return path


def _mirror_s3(secPath, bucket='bll-sec-filings'):
  '''Copies SEC filings info from S3 to local temp dir
  '''
  logger.info('Mirroring S3')
  # Create local folders
  try:
    os.makedirs(os.path.join(ROOT_DIR, secPath))
  except FileExistsError:
    pass

  # Attempt to get the index and filings from S3
  indexKey = os.path.join(secPath, 'index.gz')
  indexKeyLocal = os.path.join(ROOT_DIR, indexKey)

  try:
    s3_client.download_file(bucket, indexKey, indexKeyLocal)
  except ClientError:
    logger.info('Index file does not exisit: {}'.format(indexKey))
    pd.DataFrame(columns=['SECFNAME']).to_csv(indexKeyLocal, index=False)

  filingsKey = os.path.join(secPath, 'filings.zip')
  filingsKeyLocal = os.path.join(ROOT_DIR, filingsKey)

  try:
    logger.info('Retrieving filings archive: {}'.format(filingsKey))
    s3_client.download_file(bucket, filingsKey, filingsKeyLocal)
    _remove_bad_files_from_arcive(filingsKeyLocal)
  except ClientError:
    logger.info('Filings archive does not exist {}'.format(filingsKey))
    try:
      f = zipfile.ZipFile(filingsKeyLocal, 'w', compression=zipfile.ZIP_DEFLATED)
    except Exception as e:
      pass
    finally:
      f.close()
  except Exception as e:
    _handle_unexpected_error(e, 'Unexpected error when getting filings')

  return indexKeyLocal, filingsKeyLocal
      

def _update_s3(secPath, bucket='bll-sec-filings', index=True, filings=True):
  '''Pushes local temp dir to S3
  '''
  if index:
    logger.info('Pushing index to S3: {}'.format(secPath))
    indexKey = os.path.join(secPath, 'index.gz')
    indexKeyLocal = os.path.join(ROOT_DIR, indexKey)
    s3_client.upload_file(indexKeyLocal, bucket, indexKey)
    logger.info('Uploaded index successfully')
  else:
    indexKey = None

  if filings:
    logger.info('Pushing filings to S3: {}'.format(secPath))
    filingsKey = os.path.join(secPath, 'filings.zip')
    filingsKeyLocal = os.path.join(ROOT_DIR, filingsKey)
    s3_client.upload_file(filingsKeyLocal, bucket, filingsKey)
    logger.info('Uploaded filings successfully')
  else:
    filingsKey = None

  return indexKey, filingsKey

def _cleanup(secPath):
  '''Removes local temp dir for secPath
  '''
  logger.info('Removing local copy of index and filings: {}'.format(secPath))

  indexKey = os.path.join(secPath, 'index.gz')
  indexKeyLocal = os.path.join(ROOT_DIR, indexKey)
  os.remove(indexKeyLocal)
  logger.info('Removed local copy of filings index')

  filingsKey = os.path.join(secPath, 'filings.zip')
  filingsKeyLocal = os.path.join(ROOT_DIR, filingsKey)
  os.remove(filingsKeyLocal)
  logger.info('Removed local copy of filings')

  return True

def _download_filing(secFn, maxDepth=1):
  secBaseUrl = 'https://www.sec.gov/Archives/{}'
  filingName = secFn.split('/')[-1]
  try:
    path, res = urllib.request.urlretrieve(secBaseUrl.format(secFn), os.path.join('/tmp', filingName))
  except urllib.error.HTTPError as e:
    logger.error('Got an HTTPError: {}'.format(secFn))
    if maxDepth > 0:
      waitTime = random.uniform(5, 10)
      logger.error('Going to wait {} sec before retrying download'.format(waitTime))
      time.sleep(waitTime)
      return _download_filing(secFn, maxDepth-1)
    else:
      logger.error('Already at the maximum depth, skipping file')
      path, res = None, None
  except Exception as e:
    _handle_unexpected_error(e, 'Error while downloading {}'.format(secFn))
    path, res = None, None

  if path:
    secPath = os.path.join(*secFn.split('/')[:-1])
    filingsKeyLocal = os.path.join(ROOT_DIR, secPath, 'filings.zip')
    filingName = secFn.split('/')[-1]

    try:
      with zipfile.ZipFile(filingsKeyLocal, 'a', compression=zipfile.ZIP_DEFLATED) as filingsArchive:
        with filingsArchive.open(filingName, 'w') as filing:
          numBytes = filing.write(open(path, 'rb').read())

      os.remove(path)
    except Exception as e:
      _handle_unexpected_error(e, 'Error while adding filing to archive')
      numBytes = None
  else:
    numBytes = None
  
  return numBytes

def _build_index(secPath):
  index = pd.DataFrame(columns=['SECFNAME', 'FSIZE'])
  filingsKeyLocal = os.path.join(ROOT_DIR, secPath, 'filings.zip')

  with zipfile.ZipFile(filingsKeyLocal, 'r') as archive:
    for i, f in enumerate(archive.infolist()):
      secfname = os.path.join(secPath, f.filename)
      index.loc[i, 'SECFNAME'] = secfname
      index.loc[i, 'FSIZE'] = f.file_size

  return index

def _update_index(secPath):
  newIndex = _build_index(secPath)

  indexKey = os.path.join(secPath, 'index.gz')
  indexKeyLocal = os.path.join(ROOT_DIR, indexKey)

  newIndex.to_csv(indexKeyLocal, index=False)

  return True

def _maybe_download_filing(secFn, force=False):
  logger.info('Maybe downloading filing: {}'.format(secFn))
  # Check if the filing has been downloaded already
  secPath = os.path.join(*secFn.split('/')[:-1])
  fn = secFn.split('/')[-1]
  filingsKeyLocal = os.path.join(ROOT_DIR, secPath, 'filings.zip')

  try:
    with zipfile.ZipFile(filingsKeyLocal, 'r') as archive:
      filenames = archive.namelist()
  except FileNotFoundError as e:
    logger.error('Archive does not exist for {} : {}'.format(secFn, filingsKeyLocal))
    return None
  except Exception as e:
    _handle_unexpected_error(e, 'Unexpected error when opening filings archive')
    return None

  if fn in filenames:
    if force:
      logger.info('Filing has already been downloaded, but downloading anyway due to force being true')
      numBytes = _download_filing(secFn)
    else:
      # Filing has already been downloaded
      logger.info('Filing has already been downloaded, skipping')
      numBytes = None
  else:
    # Filing hasn't been downloaded, make sure that the path exists
    logger.info('Filing has not been downloaded, retrieving')
    numBytes =  _download_filing(secFn)

  return numBytes