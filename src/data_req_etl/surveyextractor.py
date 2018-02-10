#!/usr/bin/env python

import json
import urllib2
from os.path import expanduser
import os.path
import sys
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

from pymysql_utils1 import MySQLDB
from pymysql_utils1 import no_pwd_warnings

from string import Template
import zipfile as z
import StringIO as sio
from collections import OrderedDict
import getopt
import time
import logging
import sets
import datetime as dt
from ipToCountry import IpCountryDict
from _elementtree import parse

class QualtricsExtractor(MySQLDB):

    # NOTE: For weekly download of ALL surveys,
    #       swap the commented statements below.
     
    # Database where survey tables are to be
    # created: 
    DATA_REQUESTS_TARGET_DATABASE = 'DataRequests'
    ALL_REQUESTS_TARGET_DATABASE  = 'EdxQualtrics'
    TARGET_DATABASE = None;
    
    # Particular survey to fetch:
    DATA_REQUESTS_TARGET_SURVEY   = 'SV_ahriYiMPjMnz56l'
    ALL_REQUESTS_TARGET_SURVEY   = None

    def __init__(self, target_db=None, target_survey_id=None):
        '''
        Initializes extractor object with credentials from .ssh directory.
        Set log file directory.
        '''
        home = expanduser("~")
        userFile = home + '/.ssh/qualtrics_user'
        tokenFile = home + '/.ssh/qualtrics_token'
        dbFile = home + "/.ssh/mysql_user"
        if os.path.isfile(userFile) == False:
            sys.exit("User file not found: " + userFile)
        if os.path.isfile(tokenFile) == False:
            sys.exit("Token file not found: " + tokenFile)
        if os.path.isfile(dbFile) == False:
            sys.exit("MySQL user credentials not found: " + dbFile)
        
        self.the_survey_id = target_survey_id
        self.apiuser = None
        self.apitoken = None
        dbuser = None #@UnusedVariable
        dbpass = None #@UnusedVariable

        with open(userFile, 'r') as f:
            self.apiuser = f.readline().rstrip()

        with open(tokenFile, 'r') as f:
            self.apitoken = f.readline().rstrip()

        with open(dbFile, 'r') as f:
            dbuser = f.readline().rstrip()
            dbpass = f.readline().rstrip()

        # The commented version of the call to setupLogging()
        # writes to a nicely named file in the CWD. But this
        # class is usually used by CRON, which redirects
        # output into a file in ~/cronlog:

        self.logger = self.setupLogging()

        # self.setupLogging(loggingLevel=logging.INFO,
        #                   logFile="DataRequestsETL_%d%d%d_%d%d.log" %\
        #                     (dt.datetime.today().year, 
        #                      dt.datetime.today().month, 
        #                      dt.datetime.today().day, 
        #                      dt.datetime.now().hour, 
        #                      dt.datetime.now().minute)
        #                   )


        self.lookup = IpCountryDict()

        #****** Undo
        #*******MySQLDB.__init__(self, db="EdxQualtrics", user=dbuser, passwd=dbpass)
        #*******MySQLDB.__init__(self, port=3307, db="DataRequests", user=dbuser, passwd=dbpass)
        #MySQLDB.__init__(self, port=3307, db="DataRequests", user=dbuser, passwd=dbpass)
        MySQLDB.__init__(self, db=target_db, user=dbuser, passwd=dbpass)
        #******* End Undo

## Database setup helper methods for client

    def resetSurveys(self):
        self.execute("DROP TABLE IF EXISTS `choice`;")
        self.execute("DROP TABLE IF EXISTS `question`;")

        choiceTbl = ("""
                        CREATE TABLE IF NOT EXISTS `choice` (
                          `SurveyId` varchar(50) DEFAULT NULL,
                          `QuestionId` varchar(50) DEFAULT NULL,
                          `ChoiceId` varchar(50) DEFAULT NULL,
                          `description` varchar(3000) DEFAULT NULL
                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
                        """)

        questionTbl = ("""
                        CREATE TABLE IF NOT EXISTS `question` (
                          `SurveyID` varchar(50) DEFAULT NULL,
                          `QuestionID` varchar(5000) DEFAULT NULL,
                          `QuestionDescription` varchar(5000) DEFAULT NULL,
                          `ForceResponse` varchar(50) DEFAULT NULL,
                          `QuestionType` varchar(50) DEFAULT NULL,
                          `QuestionNumber` varchar(50) DEFAULT NULL
                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
                        """)

        self.execute(choiceTbl)
        self.execute(questionTbl)

    def resetResponses(self):
        self.execute("DROP TABLE IF EXISTS `response`;")
        self.execute("DROP TABLE IF EXISTS `response_metadata`;")
        self.execute("DROP VIEW IF EXISTS `RespondentMetadata`;")

        responseTbl = ("""
                        CREATE TABLE IF NOT EXISTS `response` (
                          `SurveyId` varchar(50) DEFAULT NULL,
                          `ResponseId` varchar(50) DEFAULT NULL,
                          `QuestionNumber` varchar(50) DEFAULT NULL,
                          `AnswerChoiceId` varchar(500) DEFAULT NULL,
                          `Description` varchar(5000) DEFAULT NULL
                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
                        """)

        responseMetaTbl = ("""
                            CREATE TABLE IF NOT EXISTS `response_metadata` (
                              `SurveyID` varchar(50) DEFAULT NULL,
                              `ResponseID` varchar(50) DEFAULT NULL,
                              `Name` varchar(1200) DEFAULT NULL,
                              `EmailAddress` varchar(50) DEFAULT NULL,
                              `IPAddress` varchar(50) DEFAULT NULL,
                              `StartDate` datetime DEFAULT NULL,
                              `EndDate` datetime DEFAULT NULL,
                              `ResponseSet` varchar(50) DEFAULT NULL,
                              `Language` varchar(50) DEFAULT NULL,
                              `ExternalDataReference` varchar(200) DEFAULT NULL,
                              `a` varchar(200) DEFAULT NULL,
                              `c` varchar(200) DEFAULT NULL,
                              `UID` varchar(200) DEFAULT NULL,
                              `userid` varchar(200) DEFAULT NULL,
                              `anon_screen_name` varchar(200) DEFAULT NULL,
                              `advance` varchar(200) DEFAULT NULL,
                              `Country` varchar(50) DEFAULT NULL,
                              `Finished` varchar(50) DEFAULT NULL,
                              `Status` varchar (200) DEFAULT NULL
                            ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
                            """)

        respondentView = ("""
                            CREATE OR REPLACE VIEW `RespondentMetadata` 
                                (SurveyId, ResponseId, anon_screen_name, Country, StartDate, EndDate)
                            AS SELECT
                                SurveyID AS SurveyId,
                                ResponseID AS ResponseId,
                                anon_screen_name,
                                Country,
                                StartDate,
                                EndDate
                            FROM response_metadata;
                           """)

        self.execute(responseTbl)
        self.execute(responseMetaTbl)
        self.execute(respondentView)

    def resetMetadata(self):
        self.execute("DROP TABLE IF EXISTS `survey_meta`;")

        surveyMeta = ("""
                        CREATE TABLE IF NOT EXISTS `survey_meta` (
                          `SurveyId` varchar(50) DEFAULT NULL,
                          `SurveyCreationDate` datetime DEFAULT NULL,
                          `UserFirstName` varchar(200) DEFAULT NULL,
                          `UserLastName` varchar(200) DEFAULT NULL,
                          `SurveyName` varchar(2000) DEFAULT NULL,
                          `responses` varchar(50) DEFAULT NULL,
                          `responses_actual` int DEFAULT NULL,
                          `course_display_name` varchar(255) DEFAULT NULL
                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
                        """)

        self.execute(surveyMeta)

## API extractor methods

    def __getSurveyMetadata(self):
        '''
        Pull survey metadata from Qualtrics API v2.4. Returns JSON object.
        '''
        url = "https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getSurveys&User=%s&Token=%s&Format=JSON&JSONPrettyPrint=1" % (self.apiuser, self.apitoken)
        data = json.loads(urllib2.urlopen(url).read())
        return data

    def __genSurveyIDs(self, forceLoad=False):
        '''
        Generator for Qualtrics survey IDs. Generates only IDs for surveys with
        new data to pull from Qualtrics unless user specifies that load should be
        forced.
        '''
        data = self.__getSurveyMetadata()
        surveys = data['Result']['Surveys']
        total = len(surveys)
        self.logger.info("Extracting %d surveys from Qualtrics..." % total)

        for idx, sv in enumerate(surveys):
            svID = sv['SurveyID']
            self.logger.info("Finding ID for survey %d out of %d total: %s" % (idx + 1, total, svID))
            if forceLoad:
                yield svID
                continue

            payload = int(sv.pop('responses', 0))
            self.logger.debug("Found %d responses." % payload)
            existing = (self.__numResponses(svID) or 0)
            self.logger.debug("Have %d responses already." % existing)
            if (existing < payload) or forceLoad:
                yield svID
            else:
                self.logger.debug("Survey %s yielded no new data." % svID)
                continue

    def __getSurvey(self, surveyID):
        '''
        Pull survey data for given surveyID from Qualtrics API v2.4. Returns XML string.
        '''
        url = "https://stanforduniversity.qualtrics.com//WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getSurvey&User=%s&Token=%s&SurveyID=%s" % (self.apiuser, self.apitoken, surveyID)
        data = urllib2.urlopen(url).read()
        try:
            return ET.fromstring(data)
        except ParseError as e:
            self.num_parse_errors += 1
            # This get caught by callers:
            raise ParseError("In __getSurvey: failed to parse surveyId %s: %s" % (surveyID, `e`))

    def __getResponses(self, surveyID):
        '''
        Pull response data for given surveyID from Qualtrics. Method generates
        JSON objects containing batches of 5000 surveys.
        '''

        self.logger.info("Getting responses for survey %s" % surveyID)

        urlTemp = Template("https://dc-viawest.qualtrics.com:443/API/v1/surveys/${svid}/responseExports?apiToken=${tk}&fileType=JSON")
        reqURL = urlTemp.substitute(svid=surveyID, tk=self.apitoken)
        req = json.loads(urllib2.urlopen(reqURL).read())

        statURL = req['result']['exportStatus'] + "?apiToken=" + self.apitoken
        percent, tries = 0, 0
        while percent != 100 and tries < 20:
            if tries > 0:
                time.sleep(5)  # Wait 5 seconds between attempts to acquire data
            try:
                stat = json.loads(urllib2.urlopen(statURL).read())
                percent = stat['result']['percentComplete']
            except:
                self.logger.warning(" Recovered from HTTP error.")
                continue
            finally:
                tries += 1
        if tries >= 20:
            self.logger.error("  Survey %s timed out." % surveyID)
            return None

        dataURL = stat['result']['fileUrl']

        # HTTP Header must include the apitoken:

        req     = urllib2.Request(dataURL)
        req.add_header("content-type", "application/json")
        req.add_header("x-api-token", self.apitoken)

        try:
            remote  = urllib2.urlopen(req).read()
        except Exception as e:
            self.logger.error("Error talking to Qualtrics server (quitting): %s" % `e`)
            sys.exit()
        dataZip = sio.StringIO(remote)
        archive = z.ZipFile(dataZip, 'r')
        dataFile = archive.namelist()[0]
        data = json.loads(archive.read(dataFile), object_pairs_hook=OrderedDict)

        if not data['responses']:
            self.logger.info("No reponses found for survey %s" % surveyID)
            return None
        else:
            self.logger.info("Done retrieving reponses for survey %s" % surveyID)
            return data


## Helper methods for interfacing with DB

    def __assignCDN(self, survey, surveyID):
        '''
        Given a Python-internalized XML structure from Qualtrics, 
        finds the embedded field 'c' and return its field value.
        That field contains the unique course identifier, which
        ties the survey to course information in the tracking log data.
        '''
        
        try:
            # The survey structure includes a substructure like:
            #    <SurveyDefinition>
            #        ...
            #        <EmbeddedData>
            #            <Field>
            #                <Name><![CDATA[c]]></Name>
            #                <Value><![CDATA[course-v1:Engineering+NuclearBrink+Fall2016]]></Value>
            #            </Field>
            # Need to find the 'c' sub-field, and the corresponding 'Value' subfield 
            
            c_fld_found = False
            course_display_name = None
            done = False
            for embeddedDataEl in survey.findall('./EmbeddedData'):
                if done:
                    break
                for fieldEl in embeddedDataEl:
                    if done:
                        break
                    for fldChild in fieldEl.iter():
                        if fldChild.tag == 'Name' and fldChild.text == 'c':
                            c_fld_found = True
                        else:
                            if c_fld_found:
                                course_display_name = fldChild.text
                                done = True
                                break
        except AttributeError as e:
            self.logger.warning("%s course identifier not resolved: %s" % (surveyID, e))
        query = "UPDATE survey_meta SET course_display_name='%s' WHERE SurveyId='%s'" % (course_display_name, surveyID)
        self.execute(query.encode('UTF-8', 'ignore'))

    def __isLoaded(self, svID):
        '''
        Checks survey_meta table for given surveyID. Returns 1 if loaded, 0 otherwise.
        '''
        return self.query("SELECT count(*) FROM survey_meta WHERE SurveyID='%s'" % svID).next()[0]

    def __numResponses(self, svID):
        '''
        Given a survey ID, fetches number of responses loaded from survey_meta table.
        '''
        return self.query("SELECT responses_actual FROM survey_meta WHERE SurveyID='%s'" % svID).next()[0]

    def __getAnonUserID(self, uid):
        '''
        Given a userID from Qualtrics, returns translated anon user ID from platform data.
        '''
        q = "SELECT edxprod.idExt2Anon('%s')" % uid
        auid = ''
        try:
            auid = self.query(q).next()[0]
        except:
            auid = 'ERROR'
        return auid


## Transform methods

    def __parseSurveyMetadata(self, rawMeta):
        '''
        Given survey metadata for active user, returns a dict of dicts mapping
        column names to values for each survey. Skips over previously loaded surveys.
        '''
        svMeta = []
        for sv in rawMeta:
            keys = ['SurveyID', 'SurveyName', 'SurveyCreationDate', 'UserFirstName', 'UserLastName', 'responses']
            data = dict()
            svID = sv['SurveyID']

            # If original call to loadSurveyMetadata() specified
            # to get only a specific survey id: check whether we
            # reached that ID; if not, keep searching:

            if self.the_survey_id is not None and svID != self.the_survey_id:
                continue

            # If this survey was loaded earlier,
            # skip it:
            if self.__isLoaded(svID):
                continue
            for key in keys:
                try:
                    val = sv[key].replace('"', '')
                    data[key] = val
                except KeyError as k:
                    data[k[0]] = 'NULL'  # Set value to NULL if no data found
            svMeta.append(data)  # Finally, add row to master dict
        return svMeta
        
    def __parseSurvey(self, svID):
        '''
        Given surveyID, parses survey from Qualtrics and returns:
         1. a dict mapping db column names to values corresponding to survey questions
         2. a dict of dicts mapping db column names to choices for each question
        Method expects an XML ElementTree object corresponding to a single survey.
        '''
        # Get survey from surveyID
        sv = None
        try:
            sv = self.__getSurvey(svID)
        except urllib2.HTTPError:
            self.logger.warning("Survey %s not found." % svID)
            return None, None
        except ParseError as e:
            self.logger.error("Survey %s could not be parsed: %s" % (svID, `e`))
            return None, None

        masterQ = dict()
        masterC = dict()

        # Handle PodioID mapping in survey_meta table
        self.__assignCDN(sv, svID)

        # Parse data for each question
        questions = sv.findall('./Questions/Question')
        for idx, q in enumerate(questions):
            parsedQ = dict()
            qID = q.attrib['QuestionID']
            parsedQ['SurveyID'] = svID
            parsedQ['QuestionID'] = qID
            parsedQ['QuestionNumber'] = q.find('ExportTag').text
            parsedQ['QuestionType'] = q.find('Type').text
            try:
                parsedQ['ForceResponse'] = q.find('Validation/ForceResponse').text
            except:
                parsedQ['ForceResponse'] = 'NULL'
            try:
                text = q.find('QuestionDescription').text.replace('"', '')
                if len(text) > 2000:
                    text = text[0:2000]
                parsedQ['QuestionDescription'] = text
            except:
                parsedQ['QuestionDescription'] = 'NULL'

            masterQ[idx] = parsedQ

            # For each question, load all choices
            choices = q.findall('Choices/Choice')
            for c in choices:
                parsedC = dict()
                cID = c.attrib['ID']
                parsedC['SurveyID'] = svID
                parsedC['QuestionID'] = qID
                parsedC['ChoiceID'] = cID
                cdesc = c.find('Description').text
                parsedC['Description'] = cdesc.replace("'", "").replace('"', '') if (cdesc is not None) else 'N/A'
                masterC[qID + cID] = parsedC

        return masterQ, masterC

    def __parseResponses(self, svID):
        '''
        Given a survey ID, parses responses from Qualtrics and returns:
        1. A list of dicts containing response metadata
        2. A list of dicts containing question responses
        Method expects a JSON formatted object with raw survey data.
        '''
        # Get responses from Qualtrics-- try multiple times to ensure API request goes through
        rsRaw = None
        for _ in range(0, 10):
            try:
                rsRaw = self.__getResponses(svID)
                break
            except urllib2.HTTPError as e:
                self.logger.error("  Survey %s gave error '%s'." % (svID, e))
                if e.getcode() == '400':
                    continue
                else:
                    return None, None

        # Return if API gave us no data
        if rsRaw is None:
            self.logger.info("  Survey %s gave no responses." % svID)
            return None, None

        # Get total expected responses
        rq = 'SELECT `responses` FROM survey_meta WHERE SurveyID = "%s"' % svID
        try:
            rnum = self.query(rq).next() #@UnusedVariable
            self.logger.info("Expecting %s responses for survey %s" % (rnum,svID))
        except StopIteration:
            self.logger.warning("Could not find number of responses for survey %s" % svID)
            return ({},{})

        self.logger.info(" Parsing %s responses from survey %s..." % (len(rsRaw['responses']), svID))

        responses = []
        respMeta = []

        for resp_from_server in rsRaw['responses']:
            # Get response metadata for each response
            # Method destructively reads question fields
            resp_meta = dict()
            resp_meta['SurveyID'] = svID
            resp_meta['ResponseID'] = resp_from_server['ResponseID']
            last_name  = resp_from_server['RecipientLastName'] if 'RecipientLastName' in resp_from_server else 'NULL'
            first_name = resp_from_server['RecipientFirstName'] if 'RecipientFirstName' in resp_from_server else 'NULL'
            resp_meta['Name'] = '%s %s' % (first_name, last_name)
            resp_meta['EmailAddress'] = resp_from_server['EmailAddress'] if 'EmailAddress' in resp_from_server else 'NULL'         
            resp_meta['IPAddress'] = resp_from_server['IPAddress'] if 'IPAddress' in resp_from_server else 'NULL'
            resp_meta['Country'] = resp_from_server['ip_country_name'] if 'ip_country_name' in resp_from_server else 'NULL'
            resp_meta['StartDate'] = resp_from_server['StartDate'] if 'StartDate' in resp_from_server else 'NULL'
            resp_meta['EndDate'] = resp_from_server['EndDate'] if 'EndDate' in resp_from_server else 'NULL'
            resp_meta['ResponseSet'] = resp_from_server['ResponseSet'] if 'ResponseSet' in resp_from_server else 'NULL'
            resp_meta['Language'] = resp_from_server['Q_Language'] if 'Q_Language' in resp_from_server else 'NULL'
            resp_meta['ExternalDataReference'] = resp_from_server['ExternalDataReference'] if 'ExternalDataReference' in resp_from_server else 'NULL'
            resp_meta['a'] = resp_from_server['a'] if 'a' in resp_from_server else 'NULL'
            resp_meta['c'] = resp_from_server['c'] if 'c' in resp_from_server else 'NULL'
            resp_meta['UID'] = resp_from_server['UID'] if 'UID' in resp_from_server else 'NULL'
            resp_meta['userid'] = resp_from_server['user_id'] if 'user_id' in resp_from_server else 'NULL'
            resp_meta['advance'] = resp_from_server['advance'] if 'advance' in resp_from_server else 'NULL'
            resp_meta['Finished'] = resp_from_server['Finished'] if 'Finished' in resp_from_server else 'NULL'
            resp_meta['Status'] = resp_from_server['Status'] if 'Status' in resp_from_server else 'NULL'

            del resp_from_server['LocationLatitude']
            del resp_from_server['LocationLongitude']
            respMeta.append(resp_meta)

            collected_keys = sets.Set(resp_meta.keys())
            all_keys = sets.Set(resp_from_server.keys())
            question_keys = all_keys - collected_keys

            # Parse remaining fields as question answers
            for question_key in question_keys:
                qs = dict()
                qs['SurveyID'] = svID
                qs['ResponseID'] = resp_from_server['ResponseID']
                qs['QuestionNumber'] = question_key
                qs['AnswerChoiceID'] = resp_from_server[question_key]
                responses.append(qs)

#             # Parse remaining fields as question answers
#             fields = resp_from_server.keys()
#             for q in fields:
#                 qs = dict()
#                 if 'Q' and '_' in q:
#                     qSplit = q.rsplit('_', 1)
#                     qNum = qSplit[0]
#                     cID = qSplit[1]
#                 else:
#                     qNum = q
#                     cID = 'NULL'
#                 qs['SurveyID'] = svID
#                 qs['ResponseID'] = resp_from_server['ResponseID']
#                 qs['QuestionNumber'] = qNum
#                 qs['AnswerChoiceID'] = cID
#                 desc = resp_from_server[q].replace('"', '').replace("'", "").replace('\\', '').lstrip('u')
#                 if len(desc) >= 5000:
#                     desc = desc[:5000]  # trim past max field length
#                 qs['Description'] = desc
#                 responses.append(qs)

        return responses, respMeta


## Convenience method for handling query calls to MySQL DB.

    def __loadDB(self, data, tableName):
        '''
        Convenience function for writing data to named table. Expects data to be
        represented as a list of dicts mapping column names to values.
        '''
        try:
            # Obtain column nmaes from the 
            # first of the survey JSON structures:
            columns = tuple(data[0].keys())
            table = []
            # self.logger.info("     " + ", ".join(columns))
            for row in data:
                vals = tuple(row.values())
                # self.logger.info("     " + ", ".join(vals))
                table.append(vals)
            if len(table) > 0:
                with no_pwd_warnings():
                    self.bulkInsert(tableName, columns, table)
        except Exception as e:
            self.logger.error("  Insert query failed: %s" % e)


## Build indexes.

    def buildIndexes(self):
        '''
        Build indexes over survey tables.
        '''
        dropIndexes = "call EdxQualtrics.dropIndexIfExists('%s.survey_meta', 'SurveyID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.survey_meta', 'SurveyID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.survey_meta', 'SurveyName');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.survey_meta', 'course_display_name');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.question', 'SurveyID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.question', 'QuestionDescription');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response_metadata', 'SurveyID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response', 'SurveyID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response', 'AnswerChoiceID');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response_metadata', 'anon_screen_name');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response', 'Description');" % QualtricsExtractor.TARGET_DATABASE +\
                      "call EdxQualtrics.dropIndexIfExists('%s.response', 'QuestionNumber');" % QualtricsExtractor.TARGET_DATABASE
                    
                    
        buildIndexes = """
                    CREATE INDEX idxSurveyMetaSurveyId
                    ON %s.survey_meta (SurveyID);"""  % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxSurveyMetaSurvNm
                    ON %s.survey_meta (SurveyName(100));"""  % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxSurveyMetaCrsName
                    ON %s.survey_meta (course_display_name(255));""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxQuestionSurveyId
                    ON %s.question (SurveyID);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE FULLTEXT INDEX idxQuestionDescription
                    ON %s.question (QuestionDescription);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxRespMetaSurveyId
                    ON %s.response_metadata (SurveyID);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxRespSurveyId
                    ON %s.response (SurveyID);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxRespChoiceId
                    ON %s.response (AnswerChoiceId(255));""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idxRespMetaAnonScrnNm
                    ON %s.response_metadata (anon_screen_name);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE FULLTEXT INDEX idResponseDescription
                    ON %s.response (Description);""" % QualtricsExtractor.TARGET_DATABASE +\
                    """
                    CREATE INDEX idResponseQuestionNum
                    ON %s.response (QuestionNumber);""" % QualtricsExtractor.TARGET_DATABASE

        dropIndexesSeq = dropIndexes.split(';')
        dropIndexesTruly = dropIndexesSeq[:-1]
        for instruction in dropIndexesTruly:
            self.execute(instruction + ';')
            
        createIndexesSeq = buildIndexes.split(';')
        createIndexesTruly = createIndexesSeq[:-1]
        for instruction in createIndexesTruly:
            self.execute(instruction + ';')

## Client data load methods

    def loadSurveyMetadata(self):
        '''
        Client method extracts and transforms survey metadata and loads to MySQL
        database using query interface inherited from MySQLDB class.
        '''
        rawMeta = self.__getSurveyMetadata()
        svMeta = rawMeta['Result']['Surveys']
        parsedSM = self.__parseSurveyMetadata(svMeta)
        if len(parsedSM) > 0:
            self.__loadDB(parsedSM, 'survey_meta')

    def loadSurveyData(self):
        '''
        Client method extracts and transforms survey questions and question
        choices and loads to MySQL database using MySQLDB class methods.
        '''
        
        if self.the_survey_id is not None:
            sids = [self.the_survey_id]
        else:
            sids = self.__genSurveyIDs(forceLoad=True)

        self.num_parse_errors = 0
        for svID in sids:
            questions, choices = self.__parseSurvey(svID)
            if (questions is None) and (choices is None):
                continue
            self.__loadDB(questions.values(), 'question')
            self.__loadDB(choices.values(), 'choice')
        self.logger.info("Encountered %s non-parsable XML responses from Qualtrics: " % self.num_parse_errors)

    def loadResponseData(self, startAfter=0):
        '''
        Client method extracts and transforms response data and response metadata
        and loads to MySQL database using MySQLDB class methods. User can specify
        where to start in the list of surveyIDs.
        '''
        # If we are loading all surveys, rather than
        # just survey that records data requests,
        # get all survey IDs;

        suspended_indexing = False

        try:
            if self.the_survey_id is not None:
                sids = [self.the_survey_id]
            else:
                sids = self.__genSurveyIDs()
                self.execute("ALTER TABLE response DISABLE KEYS")
                suspended_indexing = True
            for idx, svID in enumerate(sids):
                if idx < startAfter:
                    self.logger.info("  Skipped surveyID %s" % svID)
                    continue  # skip first n surveys
                responses, respMeta = self.__parseResponses(svID)
                retrieved = len(respMeta) if respMeta is not None else 0
                self.logger.info(" Inserting %d responses on survey %s to database." % (retrieved, svID))
                self.execute("UPDATE survey_meta SET responses_actual='%d' WHERE SurveyID='%s'" % (retrieved, svID))
                if (responses is None) or (respMeta is None):
                    continue
                self.__loadDB(responses, 'response')
                self.__loadDB(respMeta, 'response_metadata')
        finally:
            if suspended_indexing:
                self.execute("ALTER TABLE response ENABLE KEYS")

    def setupLogging(self, loggingLevel=logging.INFO, logFile=None):
        '''
        Set up the standard Python logger.

        @param loggingLevel: desired logging level threshold
        @type loggingLevel: {logging.CRITICAL |
                             logging.ERROR    |
                             logging.WARNING  |
                             logging.INFO     |
                             logging.DEBUG    |
                             logging.NOTSET}
        @param logFile: file to which logging output is replicated
        @type logFile: sting
        @returns logger
        '''

        # Create a logger with name 'survey_pull':
        logger = logging.getLogger('survey_pull')

        # Create file handler if requested:
        if logFile is not None:
            handler = logging.FileHandler(logFile)
            print('Logging will be archived in %s' % logFile)
        else:
            handler = logging.StreamHandler()

        logger.setLevel(loggingLevel)
        formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

if __name__ == '__main__':
    #************
    sys.argv.extend([
                     #'-a',
                     #'-m', 
                     #'-s', 
                     #'-r', 
                     #'-i'
                     ])
    #************
    opts, args = getopt.getopt(sys.argv[1:], 'amsrtid', 
                               ['reset', 
                                'loadmeta', 
                                'loadsurveys', 
                                'loadresponses', 
                                'responsetest', 
                                'buildindexes',
                                'datarequests'])
    if ('--datarequests','') in opts:
        # Only a subset for data sharing to extract: into DATA_REQUESTS_TARGET_DATABASE,
        qe = QualtricsExtractor(QualtricsExtractor.DATA_REQUESTS_TARGET_DATABASE,
                                QualtricsExtractor.DATA_REQUESTS_TARGET_SURVEY)
        QualtricsExtractor.TARGET_DATABASE = QualtricsExtractor.DATA_REQUESTS_TARGET_DATABASE
    else:
        # Complete survey refresh:into ALL_REQUESTSS_TARGET_DATABASE,
        qe = QualtricsExtractor(QualtricsExtractor.ALL_REQUESTS_TARGET_DATABASE,
                                QualtricsExtractor.ALL_REQUESTS_TARGET_SURVEY)
        QualtricsExtractor.TARGET_DATABASE = QualtricsExtractor.ALL_REQUESTS_TARGET_DATABASE
        
    for opt, arg in opts:
        if opt in ('-a', '--reset'):

            qe.logger.info("Resetting surveys...")
            qe.resetSurveys()
            qe.logger.info("Done resetting surveys.")

            qe.logger.info("Resetting responses...")
            qe.resetResponses()
            qe.logger.info("Done resetting responses.")

            qe.logger.info("Resetting survey metadata...")
            qe.resetMetadata()
            qe.logger.info("Done resetting survey metadata.")

        elif opt in ('-m', '--loadmeta'):

            qe.logger.info("Loading survey metadata...")
            qe.loadSurveyMetadata()
            qe.logger.info("Done loading survey metadata.")

        elif opt in ('-d', '--datarequests'):

            qe.logger.info("Resetting surveys...")
            qe.resetSurveys()
            qe.logger.info("Done resetting surveys.")

            qe.logger.info("Resetting responses...")
            qe.resetResponses()
            qe.logger.info("Done resetting responses.")

            qe.logger.info("Resetting metadata...")
            qe.resetMetadata()
            qe.logger.info("Done resetting metadata.")

            qe.logger.info("Loading survey metadata...")
            qe.loadSurveyMetadata()
            qe.logger.info("Done loading survey metadata.")

            qe.logger.info("Loading survey data...")
            qe.loadSurveyData()
            qe.logger.info("Done loading survey data.")

            qe.logger.info("Loading response data...")
            qe.loadResponseData()
            qe.logger.info("Done loading response data.")

        elif opt in ('-s', '--loadsurvey'):

            qe.logger.info("Resetting surveys...")
            qe.resetSurveys()
            qe.logger.info("Done resetting surveys.")

            qe.logger.info("Loading survey data...")
            qe.loadSurveyData()
            qe.logger.info("Done loading survey data.")

        elif opt in ('-r', '--loadresponses'):

            qe.logger.info("Loading response data...")
            qe.loadResponseData()
            qe.logger.info("Done loading response data.")

        elif opt in ('-t', '--responsetest'):

            qe.logger.info("Resetting metadata...")
            qe.resetMetadata()
            qe.logger.info("Done resetting metadata.")

            qe.logger.info("Loading survey metadata...")
            qe.loadSurveyMetadata()
            qe.logger.info("Done loading survey metadata.")

            qe.logger.info("Resetting responses...")
            qe.resetResponses()
            qe.logger.info("Done resetting responses.")

            qe.logger.info("Loading response data...")
            qe.loadResponseData()
            qe.logger.info("Done loading response data.")

        elif opt in ('-i', '--buildindexes'):

            qe.logger.info("Building indexess...")
            qe.buildIndexes()
            qe.logger.info("Done building indexess.")
