import boto3
import os
import object_storage

CONFIG={}
CONFIG['AWS_ACCESS_KEY']=os.environ.get('AWS_ACCESS_KEY', None)
CONFIG['AWS_SECRET_KEY']=os.environ.get('AWS_SECRET_KEY', None)

class object_storage_s3(object_storage.object_storage):
  def __init__(self):
    self.storage_type='s3'




def example_app():
  s3 = object_storage_s3()
  print(s3.storage_type)


if __name__ == '__main__':
  example_app()

