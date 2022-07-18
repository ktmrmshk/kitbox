import unittest
from kitbox.fakers3 import Fakers3

class TestFakers3(unittest.TestCase):
  def test_get_one(self):
    f3 = Fakers3('/Users/kitamura/.aws/credentials', 'default', 's3://kitboxtest/dir1/')
    msg = f3._get_one()
    self.assertTrue( 'ts' in msg )
    self.assertTrue( 'name' in msg )
    self.assertTrue( 'country' in msg )
    self.assertTrue( 'gender' in msg )
    self.assertTrue( 'path' in msg )
    print(msg)


  def test_records_json(self):
    f3 = Fakers3('/Users/kitamura/.aws/credentials', 'default', 's3://kitboxtest/dir1/')
    msg = f3.get_records_json(20, 0.01)
    print(msg)

  def test_filename_gen(self):
    f3 = Fakers3('/Users/kitamura/.aws/credentials', 'default', 's3://kitboxtest/dir1/')
    print(f3.filename_gen())

  def test_gen_and_upload_records_s3(self):
    f3 = Fakers3('/Users/kitamura/.aws/credentials', 'default', 's3://kitboxtest/dir1/')
    f3.gen_and_upload_records_s3()

#  def test_start(self):
#    pass


if __name__ == '__main__':
  unittest.main()
