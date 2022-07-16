# export PYTHONPATH=$(pwd)/../src/kitbox:$PYTHONPATH
import unittest

from kitbox.object_storage_s3 import object_storage_s3


class TestObjectStorageS3(unittest.TestCase):
  def setUp(self):
    self.s3 = object_storage_s3()

  def tearDown(self):
    pass

  def test_object_storage_s3(self):
    print(self.s3.storage_type)


  def test_parse_s3url(self):
    s3url = 's3://airbyte-sink/airbytedata/diamonds/2022_02_04_1643971474869_0.parquet'
    (bucket, path) = self.s3.parse_s3url(s3url)
    self.assertEqual(bucket, 'airbyte-sink')
    self.assertEqual(path, 'airbytedata/diamonds/2022_02_04_1643971474869_0.parquet')

  def test_ls(self):
    url = 's3://airbyte-sink/airbytedata/diamonds/'
    ret = self.s3.ls(url)
    print(ret)

  def test_get(self):
    src = 's3://airbyte-sink/airbytedata/diamonds/2022_02_04_1643971474869_0.parquet'
    dst = './testdata.parquet'
  
    self.s3.get(src, dst)

  def test_put(self):
    src = './testdata.parquet'
    #src = './small.dat'
    dst = 's3://kitboxtest/dir1/dir2/testdata.parquet'
    self.s3.put(src, dst)

  def test_put_object(self):
    obj = b'foobar123'
    dst = 's3://kitboxtest/dir1/dir2/foobar.txt'
    self.s3.put_object(obj=obj, dst=dst)


  def test_delete(self):

    dst = 's3://kitboxtest/dir1/dir2/testdata.parquet'
    self.s3.delete(dst)

  def test_delete_dir(self):
    d = 's3://kitboxtest/dir1/dir2/dir3/'

    self.s3.put_object(obj=b'kita123', dst=d+'1.txt')
    self.s3.put_object(obj=b'kita123', dst=d+'2.txt')
    self.s3.put_object(obj=b'kita123', dst=d+'3.txt')
    self.s3.put_object(obj=b'kita123', dst=d+'sub/5.txt')

    self.s3.delete(d, recursive=True)
  
#  def test_get_config_s3(self):
#    c1 = object_storage_s3.get_config_s3()
#    print(c1)
#
#    _s3 = object_storage_s3(c1)
#    print( _s3.ls('s3://airbyte-sink/airbytedata/diamonds/') )
    
  def test_get_config_s3_with_file(self):
    c1 = object_storage_s3.get_config_s3('./dummy_cred.txt', 'default123')
    print(c1)
    self.assertTrue( c1['aws_access_key_id'] == 'this_is_dummy1'  )
    self.assertTrue( c1['aws_secret_access_key'] == 'this_is_dummy2'  )
    self.assertTrue( c1['aws_session_token'] == None  )

if __name__ == '__main__':
  unittest.main()
