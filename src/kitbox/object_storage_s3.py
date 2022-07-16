import boto3
import os
from kitbox import object_storage
import urllib, configparser

CONFIG={}
CONFIG['aws_access_key_id']=os.environ.get('AWS_ACCESS_KEY', None)
CONFIG['aws_secret_access_key']=os.environ.get('AWS_SECRET_KEY', None)
CONFIG['aws_session_token']=os.environ.get('AWS_SESSION_TOKEN', None)


class object_storage_s3(object_storage.object_storage):
  def __init__(self, config=None):
    if not config:
      config = CONFIG

    self.storage_type='s3'
    _session = boto3.session.Session(
        aws_access_key_id = config['aws_access_key_id'],
        aws_secret_access_key = config['aws_secret_access_key'],
        aws_session_token = config['aws_session_token']
        )
    self.s3 = _session.resource('s3')

  def parse_s3url(self, s3url):
    parsed = urllib.parse.urlparse(s3url)
    bucket = parsed.netloc
    path = parsed.path[1:] # trim first '/'
    return (bucket, path)

  def ls(self, dst):
    '''
    dst: directory to list
    ex) 's3://my-bucket/foo/bar'
    '''
    bucket, path = self.parse_s3url(dst)
    files = self.s3.Bucket(bucket).objects.filter(Prefix=path) # trims first '/'
    return [f.key for f in files]

  def get(self, src, dst):
    '''
    src: s3 url
    dst: local filepath
    '''
    bucket, path = self.parse_s3url(src)
    self.s3.Bucket(bucket).download_file(path, dst) 

  def put(self, src, dst):
    '''
    src: local filepath
    dst: s3 url
    '''
    bucket, path = self.parse_s3url(dst)
    self.s3.Bucket(bucket).upload_file(src, path)

  def put_object(self, obj, dst):
    '''
    obj: binary type object for put
    dst: s3 url
    '''
    bucket, path = self.parse_s3url(dst)
    self.s3.Bucket(bucket).put_object(Body=obj, Key=path)

  def delete(self, dst, recursive=False):
    bucket, path = self.parse_s3url(dst)

    if recursive:
      self.s3.Bucket(bucket).objects.filter(Prefix=path).delete()
    else:
      self.s3.Object(bucket, path).delete()

  @staticmethod
  def get_config_s3(config_file=None, profile='default'):
    '''
    return config dict
    '''
    if not config_file:
      config_file = os.environ['HOME'] + '/.aws/credentials'

    cp = configparser.ConfigParser()
    cp.read(config_file)
    config={}
    config['aws_access_key_id']=cp[profile].get('aws_access_key_id')
    config['aws_secret_access_key']=cp[profile].get('aws_secret_access_key')
    config['aws_session_token']=cp[profile].get('aws_session_token')
    return config

def example_app():
  s3 = object_storage_s3()
  print(s3.storage_type)


if __name__ == '__main__':
  example_app()

