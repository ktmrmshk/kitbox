import boto3
import os
from kitbox import object_storage
import urllib

CONFIG={}
CONFIG['AWS_ACCESS_KEY']=os.environ.get('AWS_ACCESS_KEY', None)
CONFIG['AWS_SECRET_KEY']=os.environ.get('AWS_SECRET_KEY', None)

class object_storage_s3(object_storage.object_storage):
  def __init__(self):
    self.storage_type='s3'
    _session = boto3.session.Session(
        aws_access_key_id = CONFIG['AWS_ACCESS_KEY'],
        aws_secret_access_key = CONFIG['AWS_SECRET_KEY']
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

  def delete(self, dst):
    bucket, path = self.parse_s3url(dst)
    self.s3.Object(bucket, path).delete()


def example_app():
  s3 = object_storage_s3()
  print(s3.storage_type)


if __name__ == '__main__':
  example_app()

