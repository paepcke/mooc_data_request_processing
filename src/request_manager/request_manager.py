'''
Created on Feb 8, 2017

@author: paepcke
'''
import urllib2
from bs4 import BeautifulSoup

class DataRequestManager(object):
    '''
    classdocs
    '''
    fld_name_choice_id_dict = {
      "First Name" : 'Q_Choice1',
      "Last Name"  : 'Q_Choice2'
          # Kathy: please complete this.
      }

    def __init__(self):
        '''
        Constructor
        '''
        
    def get_request_survey(self):
      pass
    
    def get_req_info_from_admin(self):
      '''
      Present Web page, prefilled form
      with info from request. Kathy can
      fill in additions (courses shared).
      Results returned from here.
      '''
      pass
    
    def save_qualtrics_info(self, qualtrics_request_url):
      '''
      Given the URL to a filled-in request form, 
      retrieve the request form, parse the HTML,
      create name-value pairs from each field, and
      save to file. 
      '''
      req      = urllib2.Request(qualtrics_request_url)
      response = urllib2.urlopen(req)
      the_page = response.read()
      print(the_page)
      soup = BeautifulSoup(the_page, "html.parser")
      # soup('span', {'class' : 'Data'}): array of spans in order
      soup.fetch('span', {'class' : 'Data'})
      
      print(soup('Data'))
    
    def fill_in_dua_wordfile(self, courses):
      pass
    
    def check_prerequisites(self):
      pass
    
if __name__ == '__main__':
  
  data_manager = DataRequestManager()
  data_manager.save_qualtrics_info('https://proxy.qualtrics.com/proxy/?url=https%3A%2F%2Fstanforduniversity.qualtrics.com%2FCP%2FReport.php%3FSID%3DSV_ahriYiMPjMnz56l%26R%3DR_ToMk7CUJZFhCeYh&token=yDx4ygoyrTjiYkQFyAHhwiycw5TiIHXIS5orJyc1z2Q%3D')
  