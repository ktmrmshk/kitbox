import faker, json
from datetime import datetime, timezone
import random, os, time
from kitbox.object_storage_s3 import object_storage_s3


class Fakers3(object):
  def __init__(self, cred_file, cred_profile, s3_path, num_record_per_batch=100, gauss_sigma_rate=0.05, interval=10):
    self.f = faker.Faker()
    self.s3_path = s3_path
    self.num_record_per_batch = num_record_per_batch
    self.gauss_sigma_rate = gauss_sigma_rate
    self.interval = interval

    print(f'>>> {cred_file} , {cred_profile}')
    config = object_storage_s3.get_config_s3(cred_file, cred_profile)
    self.s3 = object_storage_s3(config)



  def _get_one(self):
    ts = datetime.now( timezone.utc)
    prof = self.f.profile(['name', 'sex', 'birthdate', 'mail'])
    ret = {
        'ts': ts.isoformat(),
        'name': prof['name'],
        'birthdate': prof['birthdate'].isoformat(),
        'gender': prof['sex'],
        'path': '/'+self.f.uri_path(),
        'email': prof['mail'],
        'country': self.f.country_code(),
        }
    return ret

  def get_records_json(self, count, gauss_sigma_rate=0.0):
    num_record = int( random.gauss(count, count*gauss_sigma_rate) )
    #print(num_record)
    records = str()
    for i in range(num_record):
      records += json.dumps( self._get_one() ) + '\n'
    return records

  def filename_gen(self, idx=0, ext='json'):
    '''
    20220714-2232-0001.json
    '''
    return datetime.now().strftime('%Y%m%d-%H%M') + f'-{idx:04}.{ext}'
    
  def gen_and_upload_records_s3(self, idx=0):
    rec_jsons = self.get_records_json(self.num_record_per_batch, self.gauss_sigma_rate)
    filename = self.filename_gen(idx)
    dst = os.path.join(self.s3_path, filename)
    print('DEBUG: uplaod to "{dst}"')
    self.s3.put_object( rec_jsons.encode(), dst )
    

  def start(self, start_idx=0):
    self.idx = start_idx
    while True:
      self.gen_and_upload_records_s3(self.idx)
      self.idx+=1
      print(f'index => {self.idx}')
      time.sleep(self.interval)





if __name__ == '__main__':
  f3 = Fakers3('/Users/kitamura/.aws/credentials', 'default', 's3://kitboxtest/dir1/')
  f3.start()

