import time
import trino
import pytest
import requests



    
conn = trino.dbapi.connect(host='localhost', port=4000, user='admin' )
    
# A SUCCESSFULL GET REQUEST
url_ping = "http://localhost:80/ping"
response_ping = requests.get(url_ping)
    
    
#AN UNSUCCESSFULL POST REQUEST
url_submit = "http://localhost:80/submit"
myobj = {"msg": "submit"}
response_submit = requests.post(url_submit, data = myobj)

    
    
cur1 = conn.cursor()
cur1.execute("SELECT * FROM resurface.data.message WHERE request_url = 'http://localhost/ping' ")
rows1 = cur1.fetchall()
columns1 = dict([desc[:2] for desc in cur1.description])
conn.close()
        
   
cur2 = conn.cursor()
cur2.execute("SELECT * FROM resurface.data.message WHERE request_url = 'http://localhost/submit' ")
rows2 = cur2.fetchall()
columns2 = dict([desc[:2] for desc in cur2.description])
conn.close()
        
columns = columns1
rows = rows1

def test_empty():    
	assert len(rows) > 0

def test_columns():	
	assert columns["id"] == "varchar"
	assert columns["agent_category"] == "varchar"
	assert columns["agent_device"] == "varchar"
	assert columns["agent_name"] == "varchar"
	assert columns["graphql_operation"] == "varchar"
	assert columns["graphql_operation_name"] == "varchar"
	assert columns["host"] == "varchar"
	assert columns["interval_millis"] == "bigint"
	assert columns["request_body"] == "varchar"
	assert columns["request_content_type"] == "varchar"
	assert columns["request_headers"] == "varchar"
	assert columns["request_json_type"] == "varchar"
	assert columns["request_method"] == "varchar"
#assert rows[0][12] == "GET"
	assert columns["request_params"] == "varchar"
	assert columns["request_url"] == "varchar"
#assert rows[0][14] == "http://localhost/ping"
#assert rows2[0][14] == "http://localhost/submit"
	assert columns["request_user_agent"] == "varchar"
	assert columns["response_body"] == "varchar"
#assert rows[0][15] == "{"msg": "pong"}"
	assert columns["response_code"] == "varchar"
#assert rows[0][17] == "200"
#assert rows2[0][17] == "404"
	assert columns["response_content_type"] == "varchar"
#assert rows[0][18] == "application/json"
#assert rows2[0][18] == "text/html"
	assert columns["response_headers"] == "varchar"
	assert columns["response_json_type"] == "varchar"
#assert rows[0][20] == "OBJECT"
	assert columns["response_time_millis"] == "bigint"
	assert columns["size_request_bytes"] == "integer"
	assert columns["size_response_bytes"] == "integer"
        
def test_get_404():
	assert rows[0][12] == "GET"
	assert rows[0][14] == "http://localhost/ping"
	assert rows[0][17] == "404"
	assert rows[0][18] == "text/html; charset=utf-8"
	assert rows[0][20] == None
        
def test_post_404():
	assert rows2[0][12] == "POST"
	assert rows2[0][14] == "http://localhost/submit"
	assert rows2[0][17] == "404"
	assert rows2[0][18] == "text/html; charset=utf-8"
	
	        
'''
COLUMNS
'id': 'varchar',
 'agent_category': 'varchar',
 'agent_device': 'varchar',
 'agent_name': 'varchar',
 'graphql_operation': 'varchar',
 'graphql_operation_name': 'varchar',
 'host': 'varchar',
 'interval_millis': 'bigint',
 'request_body': 'varchar',
 'request_content_type': 'varchar',
 'request_headers': 'varchar',
 'request_json_type': 'varchar',
 'request_method': 'varchar',
 'request_params': 'varchar',
 'request_url': 'varchar',
 'request_user_agent': 'varchar',
 'response_body': 'varchar',
 'response_code': 'varchar',
 'response_content_type': 'varchar',
 'response_headers': 'varchar',
 'response_json_type': 'varchar',
 'response_time_millis': 'bigint',
 'size_request_bytes': 'integer',
 'size_response_bytes': 'integer'
 
ROWS
['c6021344-83a3-40f1-a5f5-c66d7aa2331d',
  'Unknown',
  'Unknown',
  'Python-Requests',
  None,
  None,
  '44251e962b92',
  3,
  None,
  None,
  '[["host","localhost"],["accept-encoding","gzip, deflate"],["accept","*/*"],["connection","keep-alive"]]',
  None,
  'GET',
  '[]',
  'http://localhost/ping',
  'python-requests/2.22.0',
  '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<title>Error</title>\n</head>\n<body>\n<pre>Cannot GET /ping</pre>\n</body>\n</html>\n',
  '404',
  'text/html; charset=utf-8',
  '[["x-powered-by","Express"],["access-control-allow-origin","*"],["content-security-policy","default-src \\u0027none\\u0027"],["x-content-type-options","nosniff"],["content-length","143"]]',
  None,
  1626888160009,
  123,
  313]
-------------------------------
response_ping.__getstate__()

{{'_content': b'<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<title>Error</title>\n</head>\n<body>\n<pre>Cannot GET /ping</pre>\n</body>\n</html>\n',
 'status_code': 404,
 'headers': {'X-Powered-By': 'Express', 'Access-Control-Allow-Origin': '*', 'Content-Security-Policy': "default-src 'none'", 'X-Content-Type-Options': 'nosniff', 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': '143', 'Date': 'Wed, 21 Jul 2021 17:27:18 GMT', 'Connection': 'keep-alive', 'Keep-Alive': 'timeout=5'},
 'url': 'http://localhost:80/ping',
 'history': [],
 'encoding': 'utf-8',
 'reason': 'Not Found',
 'cookies': <RequestsCookieJar[]>,
 'elapsed': datetime.timedelta(microseconds=3376),
 'request': <PreparedRequest [GET]>}  
------------------------------
response_submit.__getstate__()

{'_content': b'<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<title>Error</title>\n</head>\n<body>\n<pre>Cannot POST /submit</pre>\n</body>\n</html>\n',
 'status_code': 404,
 'headers': {'X-Powered-By': 'Express', 'Access-Control-Allow-Origin': '*', 'Content-Security-Policy': "default-src 'none'", 'X-Content-Type-Options': 'nosniff', 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': '146', 'Date': 'Wed, 21 Jul 2021 17:27:18 GMT', 'Connection': 'keep-alive', 'Keep-Alive': 'timeout=5'},
 'url': 'http://localhost:80/submit',
 'history': [],
 'encoding': 'utf-8',
 'reason': 'Not Found',
 'cookies': <RequestsCookieJar[]>,
 'elapsed': datetime.timedelta(microseconds=2676),
 'request': <PreparedRequest [POST]>}




'''

        

