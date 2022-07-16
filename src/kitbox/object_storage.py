### BASE CLASS FOR INTERFACE

class object_storage(object):
  def __init__(self):
    self.storage_type=None # s3, gcs, blob
  
  def ls(self, dst):
    pass

  def get(self, src, dst):
    pass

  def put(self, src, dst):
    pass

  def put_object(self, obj, dst):
    pass

  def delete(self, dst):
    pass
