#!/usr/bin/env python
import os

import hashlib
import boto
import subprocess
import tempfile
import uuid
import zlib
import time
import logging
import threadpool
import baker
import logging
import threading

logging.basicConfig(level=logging.INFO)

### Classes

class Backup(object):

	def __init__(self, path, bucket):
		self.path = path
		self.bucket = bucket
	
	def start(self):
		
		files = fileList(self.path)
		
		self.stats = {
			'files': len(files),
			'start': time.time(),
			'bytes': 0,
			'done':  0
		}
		
		mapAsync(self.handleFile, files)
	
	def handleFile(self, path):

		size = os.path.getsize(path)
		info = uploadFile(path, self.bucket)

		self.stats['bytes'] += size
		self.stats['done']  += 1
		
		char = '+'
		if info['skip']: char = '=' 
		
		speed = self.stats['bytes'] / (time.time() - self.stats['start'])

		logging.info('%s %s/%s %s (%s) %s/s %s', char, self.stats['done'], self.stats['files'], readableBytes(self.stats['bytes']), readableBytes(size), readableBytes(speed), path)

### Utility methods

def readableBytes(bytes):
	bytes = float(bytes)
	if bytes >= 1099511627776:
		terabytes = bytes / 1099511627776
		size = '%.2fT' % terabytes
	elif bytes >= 1073741824:
		gigabytes = bytes / 1073741824
		size = '%.2fG' % gigabytes
	elif bytes >= 1048576:
		megabytes = bytes / 1048576
		size = '%.2fM' % megabytes
	elif bytes >= 1024:
		kilobytes = bytes / 1024
		size = '%.0fK' % kilobytes
	else:
		size = '%.0fb' % bytes
	return size

def fileList(path):
	
	paths = []
	
	for fileName in os.listdir(unicode(path)):
		
		filePath = os.path.join(path, fileName)
		
		if os.path.isdir(filePath):
			paths += fileList(filePath)
		else:
			paths.append(filePath)
	
	return sorted(list(set(paths)))

def fileChecksum(path, blockSize=4096):
	
	md5 = hashlib.md5()
	f = open(path, 'r')
	while True:
		data = f.read(blockSize)
		if not data: break
		md5.update(data)
	f.close()

	return md5.hexdigest()

def compressString(s):
	"""Gzip a given string."""
	import cStringIO, gzip
	
	zbuf = cStringIO.StringIO()
	zfile = gzip.GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
	zfile.write(s)
	zfile.close()
	return zbuf.getvalue()

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2):
	"""Retry decorator
	original from http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
	"""
	def deco_retry(f):
		def f_retry(*args, **kwargs):
			mtries, mdelay = tries, delay
			while mtries > 0:
				try:
					return f(*args, **kwargs)
				except ExceptionToCheck, e:
					logging.error("%s, Retrying in %d seconds...", str(e), mdelay)
					time.sleep(mdelay)
					mtries -= 1
					mdelay *= backoff
					lastException = e
			raise lastException
		return f_retry # true decorator
	return deco_retry

def mapAsync(f, collection, threads=8):

	pool = threadpool.ThreadPool(threads)
	
	for req in threadpool.makeRequests(f, collection):
		pool.putRequest(req)
		
	pool.wait()
	del pool


def getAwsBucket(accessKey, secretKey, awsBucketName):

	connection = boto.connect_s3(accessKey, secretKey)

	try:
		awsBucket = connection.create_bucket(awsBucketName)
	except boto.exception.S3CreateError, e:
		raise NameError("Could not create bucket %s: %s" % (awsBucketName, e))
		
	return awsBucket


@retry(Exception, tries=3)
def uploadFile(path, awsBucket):
	
	info = {
		'checksum': fileChecksum(path),
		'compress': os.path.splitext(path)[1] not in ['.zip', '.gzip', '.tgz'],
		'bytes': os.path.getsize(path),
		'skip': False,
		'remote-checksum': None
	}
	
	if info['compress']:
		destPath = path + u'.gz'
	else:
		destPath = path
	
	key = awsBucket.get_key(destPath)
	
	if not key:
		key = awsBucket.new_key(destPath)
	else:
		info['remote-checksum'] = key.get_metadata('meta-checksum')
	
	if str(info['remote-checksum']) == str(info['checksum']):
		info['skip'] = True
	
	else:
		key.set_metadata('meta-checksum', info['checksum'])
	
		if not info['compress']:
			key.set_contents_from_filename(path)
		
		else:
		
			if os.path.getsize(path) > 1024 * 1024: # One megabyte
				tempFilePath = os.path.join('/tmp', '%s-%s.gz' % (uuid.uuid4().hex, info['checksum']))
				logging.info('large gzip %s to %s', path, tempFilePath)
				subprocess.call('gzip -c "%s" > %s' % (path, tempFilePath), shell=True)
				key.set_contents_from_filename(tempFilePath)
				info['bytes'] = os.path.getsize(tempFilePath)
				logging.info('remove %s' % tempFilePath)
				# os.remove(tempFilePath)
			
			else:
				f = open(path, 'r')
				data = compressString(f.read())
				key.set_contents_from_string(data)
				info['bytes'] = len(data)
				f.close()
	
	return info
		
### Main program

@baker.command
def backup(path, aws_access_key=None, aws_secret_key=None, aws_bucket_name=None):
	
	if not aws_access_key:
		aws_access_key = raw_input('Amazon access key: ')

	if not aws_secret_key:
		aws_secret_key = raw_input('Amazon secret key: ')

	if not aws_bucket_name:
		aws_bucket_name = raw_input('Amazon S3 bucket name key: ')
	
	bucket = getAwsBucket(aws_access_key, aws_secret_key, aws_bucket_name)
	backup = Backup(path, bucket)
	
	backup.start()
	

baker.run()

