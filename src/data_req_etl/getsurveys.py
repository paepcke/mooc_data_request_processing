import json
import urllib2
url="""https://stanforduniversity.qualtrics.com/WRAPI/ControlPanel/api.php?API_SELECT=ControlPanel&Version=2.4&Request=getSurveys&User=jagadish%40stanford.edu%23stanforduniversity&Token=3dC3aLzMa73YXK4GU9BYBpdj5TwzSvv0o5XgYpYE&Format=JSON&JSONPrettyPrint=1"""
d=urllib2.urlopen(url).read()
dict=json.loads(d)
print dict
