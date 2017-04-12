'''
Created on Feb 8, 2017

@author: paepcke
'''

import os
import sys
import urllib2

# from MySQLdb import Warning as db_warning
# from MySQLdb import _mysql
# from MySQLdb.cursors import DictCursor as DictCursor
# from MySQLdb.cursors import SSCursor as SSCursor
# from MySQLdb.cursors import SSDictCursor as SSDictCursor
import mysql
from mysql import connector
from bs4 import BeautifulSoup


class DataRequestManager(object):
    '''
    classdocs
    '''
    # Particular survey to fetch:
    TARGET_SURVEY   = 'SV_ahriYiMPjMnz56l'
    
    fld_name_choice_id_dict = {
      "First Name" : 'Q_Choice1',
      "Last Name"  : 'Q_Choice2'
          # Kathy: please complete this.
      }

    def __init__(self):
        '''
        Constructor
        '''
        try:
            HOME = os.getenv('HOME')
            pwd_file = os.path.join(HOME, '.ssh/mysql')
            with open(pwd_file, 'r') as file_handle:
                self.pwd = file_handle.read().strip()
        except Exception:
            print('Cannot obtain MySQL password.')
            sys.exit(1)
        
        self.mysql_user = 'dataman'
        self.get_request_survey(self.mysql_user, self.pwd)
        
        
    def get_request_survey(self, mysql_user, mysql_pwd):
        
        #conn = _mysql.connect(host='localhost',
        conn = connector.connect(host='localhost',
                                 user=mysql_user, 
                                 passwd=mysql_pwd,
                                 db='DataRequests'
                                 )

        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM survey_meta')
        one_row_dict = cursor.fetchone()
        print(one_row_dict)
    
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
    