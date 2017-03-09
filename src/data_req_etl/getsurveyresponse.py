import json
import urllib2
url="""https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getLegacyResponseData&User=jagadish%40stanford.edu%23stanforduniversity&Token=3dC3aLzMa73YXK4GU9BYBpdj5TwzSvv0o5XgYpYE&Format=JSON&SurveyID=SV_6YegHjmjmngyhVz&Labels=1"""
d=urllib2.urlopen(url).read()
print 'ELABORATE RESPONSE'
print d
print 'CONDENSED RESPONSE'
url="""https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getLegacyResponseData&User=jagadish%40stanford.edu%23stanforduniversity&Token=3dC3aLzMa73YXK4GU9BYBpdj5TwzSvv0o5XgYpYE&Format=JSON&SurveyID=SV_6YegHjmjmngyhVz"""
d=urllib2.urlopen(url).read()
print d
