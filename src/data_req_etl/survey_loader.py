import json
import urllib2
from pymysql_utils1 import MySQLDB
#from pymysql_utils import MySQLDB
import xml.etree.ElementTree as ET
from urllib2 import Request, URLError
from os.path import expanduser
import os.path
import sys

"""
 Ensures that the EdxQualtrics database, and all required tables are present: 
"""

def initDatabaseIfNeeded():

    #db.execute("CREATE DATABASE IF NOT EXISTS EdxQualtrics;")
    db.execute("DROP TABLE IF EXISTS `choice`;")
    db.execute("DROP TABLE IF EXISTS `question`;")
    db.execute("DROP TABLE IF EXISTS `response`;")
    db.execute("DROP TABLE IF EXISTS `response_metadata`;")
    db.execute("DROP TABLE IF EXISTS `survey_meta`;")
    
    choiceTbl   =      "CREATE TABLE IF NOT EXISTS `choice` (" +\
                        "`SurveyId` varchar(50) DEFAULT NULL," +\
                        "`QuestionId` varchar(50) DEFAULT NULL," +\
                        "`ChoiceId` varchar(50) DEFAULT NULL," +\
                        "`description` varchar(3000) DEFAULT NULL" +\
                        ") ENGINE=MyISAM DEFAULT CHARSET=utf8;"
                 
    questionTbl =     "CREATE TABLE IF NOT EXISTS `question` (" +\
 					    "`SurveyId` varchar(50) DEFAULT NULL," +\
 					    "`questionid` varchar(5000) DEFAULT NULL," +\
 					    "`questiontext` varchar(5000) DEFAULT NULL," +\
 					    "`questiondescription` varchar(5000) DEFAULT NULL," +\
 					    "`ForceResponse` varchar(50) DEFAULT NULL," +\
 					    "`QuestionType` varchar(50) DEFAULT NULL" +\
 					  ") ENGINE=MyISAM DEFAULT CHARSET=utf8;"
 
    responseTbl =     "CREATE TABLE IF NOT EXISTS `response` (" +\
    					"  `SurveyId` varchar(50) DEFAULT NULL," +\
    					"  `ResponseId` varchar(50) DEFAULT NULL," +\
    					"  `QuestionId` varchar(50) DEFAULT NULL," +\
    					"  `AnswerChoiceId` varchar(500) DEFAULT NULL," +\
    					"  `Description` varchar(2000) DEFAULT NULL" +\
    					") ENGINE=MyISAM DEFAULT CHARSET=utf8;"
                         
    responseMetaTbl = "CREATE TABLE IF NOT EXISTS `response_metadata` (" +\
 					  "  `SurveyId` varchar(50) DEFAULT NULL," +\
 					  "  `name` varchar(1200) DEFAULT NULL," +\
 					  "  `EmailAddress` varchar(50) DEFAULT NULL," +\
 					  "  `IpAddress` varchar(50) DEFAULT NULL," +\
 					  "  `StartDate` datetime DEFAULT NULL," +\
 					  "  `EndDate` datetime DEFAULT NULL," +\
 					  "  `anon_id` varchar(40) DEFAULT NULL," +\
 					  "  `ext_id` varchar(40) DEFAULT NULL" +\
 					  ") ENGINE=MyISAM DEFAULT CHARSET=utf8;"
 					                          
    surveyMeta   =   "CREATE TABLE IF NOT EXISTS `survey_meta` (" +\
 					 "  `SurveyId` varchar(50) DEFAULT NULL," +\
 					 "  `SurveyCreationDate` datetime DEFAULT NULL," +\
 					 "  `userfirstname` varchar(200) DEFAULT NULL," +\
 					 "  `userlastname` varchar(200) DEFAULT NULL," +\
 					 "  `surveyname` varchar(2000) DEFAULT NULL," +\
 					 "  `SurveyOwnerId` varchar(50) DEFAULT NULL," +\
 					 "  `SurveyExpirationDate` datetime DEFAULT NULL" +\
 					 ") ENGINE=MyISAM DEFAULT CHARSET=utf8;"

    db.execute(choiceTbl)
    db.execute(questionTbl)
    db.execute(responseTbl)
    db.execute(responseMetaTbl)
    db.execute(surveyMeta)
                                              						
"""
Returns User, Token pair by parsing the file qualtrics_user and qualtrics_token
in the ~/.ssh directory.
"""
def getUserPwd():
  home = expanduser("~")
  userFile = home + '/.ssh/qualtrics_user'  
  tokenFile = home + '/.ssh/qualtrics_token'
  if os.path.isfile(userFile) == False:
    sys.exit("User file not found " + userFile) 
  if os.path.isfile(tokenFile) == False:
    sys.exit("Token file not found " + tokenFile) 

  uid = None
  token = None  

  with open(userFile, 'r') as f:
    uid = f.readline()

  with open(tokenFile, 'r') as f:
    token = f.readline()
  
  return uid.strip(), token.strip()

_User,_Token = getUserPwd()
#********
# print _User, _Token
#********


"""
Returns User, Token pair by parsing the file qualtrics_user and qualtrics_token
in the ~/.ssh directory.
"""
def getMysqlUserPwd():
  home = expanduser("~")
  userFile = home + '/.ssh/mysql_qualtrics_user'  
  tokenFile = home + '/.ssh/mysql_qualtrics_pwd'
  if os.path.isfile(userFile) == False:
    sys.exit("User file not found " + userFile) 
  if os.path.isfile(tokenFile) == False:
    sys.exit("Token file not found " + tokenFile) 

  uid = None
  token = None  

  with open(userFile, 'r') as f:
    uid = f.readline()

  with open(tokenFile, 'r') as f:
    token = f.readline()
  
  return uid.strip(), token.strip()

_User,_Token = getUserPwd()
#********
# print _User, _Token
#********

user,pwd = getMysqlUserPwd()
#db=MySQLDB('127.0.0.1',3306,'root','','EdxQualtrics')
# Now connect to the qualtrics db; NOTE the
# db EdxQualtrics must be present:
db=MySQLDB('127.0.0.1',3306,user,pwd,'EdxQualtrics')
initDatabaseIfNeeded()


"""
{u'Meta': {u'Status': u'Success', u'Debug': u''}, u'Result': {u'Surveys': [{u'SurveyCreationDate': u'2014-10-27 15:08:19', u'responses': u'3', u'UserFirstName': u'jagadish', u'SurveyType': u'SV', u'LastModified': u'2014-11-04 12:50:51', u'LastActivated': u'2014-10-27 15:21:36', u'SurveyName': u'cs140-feedback', u'UserLastName': u'venkatraman', u'SurveyID': u'SV_6YegHjmjmngyhVz', u'SurveyStatus': u'Active', u'CreatorID': u'UR_elipjMDGVTxcjZz', u'SurveyStartDate': u'0000-00-00 00:00:00', u'SurveyOwnerID': u'UR_elipjMDGVTxcjZz', u'SurveyExpirationDate': u'0000-00-00 00:00:00'}]}}
Returns all surveys for a particular user, token pair
"""
def getSurveysForUser (User, Token):
  url='https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getSurveys&User=%s&Token=%s&Format=JSON&JSONPrettyPrint=1'%(User,Token)
  #********
  # print url
  #********
  d=None
  try:
    d=urllib2.urlopen(url).read()
  except URLError as e:
    if hasattr(e, 'reason'):
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
    elif hasattr(e, 'code'):
        print 'The server couldn\'t fulfill the request.'
        print 'Error code: ', e.code
    return None
    # everything is fine

  dict=json.loads(d)
  #********
  # print dict
  #********
  return dict

"""
Returns all details corresponding to a survey. Fires up a request to the qualtrics
getSurvey() API and returns the raw XML. The format of the XML is specific to qualtrics
"""
def getSurvey (User, Token, SurveyID):
  'SV_6YegHjmjmngyhVz'
  url="https://stanforduniversity.qualtrics.com//WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getSurvey&User=%s&Token=%s&SurveyID=%s"%(User,Token, SurveyID)
  #********
  # print url
  #********
  d=None
  try:
    d=urllib2.urlopen(url).read()
  except URLError as e:
    if hasattr(e, 'reason'):
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
    elif hasattr(e, 'code'):
        print 'The server couldn\'t fulfill the request.'
        print 'Error code: ', e.code
    return None


  #********
  # print d
  #********
  return d

"""
Obtains all responses corresponding to a survey
Both elaborate_response and condensed_response response-types are supported.
"""
def getResponses (User, Token, SurveyID):
  url="""https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getLegacyResponseData&User=%s&Token=%s&Format=JSON&SurveyID=%s&Labels=1"""%(User, Token, SurveyID)
  #********
  # print url
  #********
  try:
    elaborate_response=json.loads(urllib2.urlopen(url).read())
  except URLError as e:
    if hasattr(e, 'reason'):
        print 'We failed to reach a server.'
        print '    Reason: ', e.reason
    elif hasattr(e, 'code'):
        print 'The server couldn\'t fulfill the request.'
        print '    Error code: ', e.code
        return None, None
  except ValueError as e:
       print "Bad json in getResponses() elaborate response: '%s'" % str(urllib2.urlopen(url).read())
       return None, None


  #********
  # print 'ELABORATE RESPONSE'
  # print elaborate_response

  # print 'CONDENSED RESPONSE'
  #********
  url="""https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getLegacyResponseData&User=%s&Token=%s&Format=JSON&SurveyID=%s"""%(User, Token, SurveyID)  
  #********
  # print url
  #********

  try:
    condensed_response=json.loads(urllib2.urlopen(url).read())
  except URLError as e:
    if hasattr(e, 'reason'):
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
    elif hasattr(e, 'code'):
        print 'The server couldn\'t fulfill the request.'
        print 'Error code: ', e.code
    return None, None
  except ValueError as e:
       print "Bad json in getResponses() condensed response: '%s'" % str(urllib2.urlopen(url).read())
       return None, None


  #********
  # print condensed_response
  #********

  return (elaborate_response,condensed_response)

"""
Transformation function Transform(x): returns x if x is not None, 
                              returns 'NULL' if x is None
"""
def Transform(x):
  if x is None:
    return '"NULL"'
  return x

def MkSafeStr(x):
    if x is None:
        return x

    try:
      s = str(x)
    except :
      s = ''

    s = s.replace('"','""')
    s = s.replace ("'",'""')
    s = '"%s"'%(s)
    return s

"""
Parses the responses and extracts fields like ip, name, anon etc from the response.
Also parses responses and inserts each response to a question in the DB
"""
def parseResponses (SurveyID):
  responses, condensed_responses = getResponses(_User, _Token, SurveyID)
  if responses == None or condensed_responses == None:
      return
  if (not(isinstance(responses,dict))):
      return

  responseIds = responses.keys()

  for responseId in responseIds:
    response = responses[responseId]
    ip=response['IPAddress']
    name=response['Name']
    end=response['EndDate']
    email=response['EmailAddress']
    ext_id = ''
    if 'a' in response:
      ext_id = response['a']
    start=response['StartDate']
    #********
    # print ext_id
    #********
    query = "insert into response_metadata(SurveyId, Name, EmailAddress, IpAddress, StartDate, EndDate,  ext_id, anon_id) values\
    ('%s','%s','%s','%s','%s','%s','%s',idExt2Anon('%s'))"%(MkSafeStr(SurveyID),MkSafeStr(name),MkSafeStr(email),MkSafeStr(ip),start,end, ext_id, ext_id)
    #********
    # print query
    #********
    db.execute(query.encode('UTF-8','ignore'))
    if (not(isinstance(response,dict))):
      continue

    keys = response.keys()
    for key in keys:
      #********
      # print key
      # print response
      #********

      if key and key[0]=='Q' and '_' in key:
        splits = key.split('_')
        #qid = key[0]
        qid = splits[0]
        cid = splits[1]
        ans = response[key]
        query = "insert into response(SurveyId, ResponseId, QuestionId, AnswerChoiceId,Description) values \
         ('%s','%s','%s','%s', '%s')" % (MkSafeStr(SurveyID), MkSafeStr(responseId), MkSafeStr(qid), MkSafeStr(cid),MkSafeStr(ans))
        #********
        # print query
        #********
        db.execute (query.encode('UTF-8','ignore'))
      else:
          if key and key[0]=='Q':
            qid = key
            ans = response[key]
            query = "insert into response(SurveyId, ResponseId, QuestionId, Description) values \
             ('%s','%s','%s','%s')" % (MkSafeStr(SurveyID), MkSafeStr(responseId), MkSafeStr(qid), MkSafeStr(ans))
            #********
            # print query
            #********
            db.execute (query.encode('UTF-8','ignore'))

"""
Driver for executing the program, that parses the survey questions and loads the Survey
responses
"""

try:
    d=getSurveysForUser (_User, _Token)  
except ValueError as e:
    print("Bad json in user surveys dict (user '%s'; token '%s'): %s" % (_User, _Token, `e`))

if d == None:
  print 'no surveys found for user %s' %(_User)
  sys.exit()
  
#********
# print d
#********
res = d['Result']
if 'Surveys' not in res:
	raise  Exception('ERROR. Surveys is not in results')
surveys = res['Surveys']
for survey in surveys:
  #********
  print("Processing survey '%s' (SurveyId: '%s')" % (survey['SurveyName'], survey.get('SurveyID','n/a')))
  #********
  q="insert into survey_meta(SurveyId, SurveyCreationDate, UserFirstName, UserLastName, SurveyName, SurveyOwnerId, SurveyExpirationDate) values\
  ('%s','%s','%s','%s','%s','%s','%s') "% \
  (MkSafeStr(survey.get('SurveyID','n/a')),
   survey.get('SurveyCreationDate','n/a'),
   MkSafeStr(survey.get('UserFirstName','n/a')),
   MkSafeStr(survey.get('UserLastName','n/a')),
   MkSafeStr(survey.get('SurveyName','n/a')),
   MkSafeStr(survey.get('SurveyOwnerID','n/a')),
   survey.get('SurveyExpirationDate','n/a'))
  db.execute(q.encode('UTF-8','ignore'))
  x=getSurvey(_User,_Token,survey['SurveyID'])
  
  if x == None:
    continue

  sid = survey['SurveyID']
  root = ET.fromstring(x)
  questionElements = root.findall('./Questions/Question')
  for q in questionElements:
    qid=q.attrib['QuestionID']
    type_=q.find('Type').text
    text=q.find('QuestionText').text
    desc=q.find('QuestionDescription').text
    forceElem = q.find('Validation/ForceResponse')
    if forceElem is not None:  
      force=q.find('Validation/ForceResponse').text
    else:
      force = 'False'  

    query= "insert into question(SurveyId, QuestionId, QuestionText, QuestionDescription, ForceResponse, Questiontype) values ('%s','%s','%s','%s','%s','%s')\
    "%(sid, qid, MkSafeStr(text), MkSafeStr(desc), force, type_)
    #********
    # print query
    #********
    db.execute(query.encode('UTF-8','ignore'))
    choices = q.findall('Choices/Choice')
    for choice in choices:
       cid = choice.attrib['ID']
       cdesc = choice.find('Description').text
       if cdesc is None:
           cdesc = 'n/a'
       else:
           cdesc=cdesc.replace("'","''")
       query= "insert into choice(SurveyId, QuestionId, ChoiceId, Description) values ('%s','%s','%s','%s')"%(sid, qid, cid, MkSafeStr(cdesc))
       #********
       # print query
       #********
       db.execute(query.encode('UTF-8','ignore'))

  try:
      parseResponses(sid)
  except ValueError as e:
      print("Bad json in survey '%s' (%s): '%s'" % (survey['SurveyName'], `e`, str(sid)))
      continue
      


