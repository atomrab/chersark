import os
import sys
import re
import errno
import requests
import BeautifulSoup
import getpass
import sqlite3
import json
import copy
import time
import datetime
import random

BS = BeautifulSoup.BeautifulSoup

def process_img(link, depth):
    link = link.replace(ark_root, "")
    new_link = ""
    for depth in range(depth):
       new_link += "../"
    new_link += link.strip("/")
    return new_link

def process_href(link, depth):
    if(link[0:7]=='http://' or link[0:8]=='https://'):
        return link
    link = link.split("?")[0]
    link = link.replace(ark_root, "").replace(".php","")
    new_link = ""
    for depth in range(depth):
       new_link += "../"
    new_link += link.strip("/")
    if (new_link[-5:]!=".html" and new_link[-3:]!=".js" and new_link[-4:]!=".jpg"):
        new_link = new_link +".html"
    return new_link

#getJson wraps requests with some useful checks and returns parsed Json
def getJson(url, cookie = False, values = False, username=False, password=False):
    verify=True
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    try:
        if (username and password and values):
            r = requests.post(url, data=values, auth=(username, password),verify=verify)
        elif (username and password):
            r = requests.get(url, auth=(username, password),verify=verify)
        elif (cookie and values):
            requrl ="{}?{}".format(url,"&".join(['{}={}'.format(k,v) for k,v in values.iteritems()]))
            r = requests.post(requrl, json=json.dumps(values), cookies=cookie,headers=headers,verify=verify)
        elif (values):
            r = requests.post(url, data=values,verify=verify,cookies=cookie)
        else:
            r = requests.get(url,verify=verify,cookies=cookie)
    except:
        sleepsec = random.random()*10
        print "Connection Error, retrying in {}s".format(round(sleepsec,2))
        sys.stdout.flush()
        time.sleep(sleepsec)
        return getJson(url, cookie, values, username, password)
    try:
        return r.json()
    except ValueError, e:
        print "JSON ERROR on request: {} with {}".format(url,values)
        try:
            return json.loads(r.text.split("<pre></pre>",1)[1])
        except IndexError, e:
            time.sleep(1)
            print "got: {}".format(r.text)
            return getJson(url, cookie, values)
    return False


def processPage(soup, depth, follow = True):

    anchors = soup.findAll("a")
    for anchor in anchors:
        try:
            raw_anchor = anchor['href']
        except KeyError:
            print("error on {}".format(anchor))
            continue
        anchor['href'] = process_href(anchor['href'], depth)

        atrsearch = re.search('search/atr/([0-9]+)/([0-9]+)/',anchor['href'])
        if(atrsearch and follow):
            atrsearchurl = root_url.strip("/")+raw_anchor.strip().replace(ark_root,"")
            cursor.execute('''SELECT count(*) from pages where url = ?''', (atrsearchurl,))
            count = cursor.fetchone()[0]
            if count is 0 :
                checkComplete(atrsearchurl)
        fauxdex = re.search('fauxdex', anchor['href'])
        if(fauxdex and follow):
            fauxdexurl = root_url.strip("/")+raw_anchor.strip().replace(ark_root,"")
            cursor.execute('''SELECT count(*) from pages where url = ?''', (fauxdexurl,))
            count = cursor.fetchone()[0]
            if count is 0 :
                checkComplete(fauxdexurl)

        allbutton = re.search('search/atr/([0-9]+)/([0-9]+)/1/all', anchor['href'])
        if(allbutton):
            showallurl = root_url.strip("/")+raw_anchor.strip().replace(ark_root,"")
            cursor.execute('''SELECT count(*) from pages where url = ?''', (showallurl,))
            count = cursor.fetchone()[0]
            if count is 0 :
                checkComplete(showallurl)

        allbutton = re.search('search/key/([0-9]+)/([0-9all]+)/all', anchor['href'])
        if(allbutton):
            showallurl = root_url.strip("/")+raw_anchor.strip().replace(ark_root,"")
            cursor.execute('''SELECT count(*) from pages where url = ?''', (showallurl,))
            count = cursor.fetchone()[0]
            if count is 0 :
                checkComplete(showallurl)

    images = soup.findAll("img")
    for image in images:
        image['src'] = process_img(image['src'], depth)

    links = soup.findAll("link")
    for link in links:
        link['href'] = process_img(link['href'], depth)

    scripts = soup.findAll("script",{"src":True})
    for script in scripts:
        raw_script = script['src']
        script['src'] = process_href(script['src'], depth)
    
    prettysoup =  soup.prettify()

    prettysoup = prettysoup.replace(" &amp;&amp; "," && ").replace("resolution &gt; cutoff","resolution > cutoff").replace("index&lt;0","index<0").replace("ht &gt; largest_child","ht > largest_child").replace("index&gt;iframeids","index>iframeids").replace("i&lt;features.length","i < features.length").replace("i &lt; features.length","i < features.length").replace("i &lt; rulesarr.length","i < rulesarr.length").replace("canvas.width &gt; 0","canvas.width > 0")
    return prettysoup

def updateScreen(message):
    sys.stdout.write("\033[K")
    sys.stdout.write("\r")
    sys.stdout.flush()
    sys.stdout.write( message)
    sys.stdout.flush()

def getPage(url, cookies):

    depth = len(url.split("/"))-1

    updateScreen("downloading %s" % url)

    requrl = url if(url[0:4]=='http') else  root_url+url

    r = requests.post(requrl, cookies=cookies)

    updateScreen("souping %s" % url)

    soup = BS(r.text)

    updateScreen("processing soup %s" % url)

    processed_soup = processPage(soup, depth)

    updateScreen(" ")

    updateScreen("writing %s" % url)

    filename = destination+ url.replace(root_url,"").replace(".php","")+'.html'
    with open(filename, 'wb') as f:
        f.write(processed_soup)
    updateScreen('Saved file %s' % filename)

    cursor.execute('''SELECT count(*) from pages where downloaded=1''')

    row = cursor.fetchone()

    numPages = getMetadata("numberOfPages")

    print("")

    print("page {0} of {1} {2}%".format(row[0], numPages, round((float(row[0])/float(numPages))*100 )))

    return False

def initProgressDb(location):
    # Creates or opens a file called mydb with a SQLite3 DB
    name = location[:-1]
    print name
    try:
        open(name+'_progress.sqlite')
        db = sqlite3.connect(name+'_progress.sqlite')
		
        cursor = db.cursor()

        def insertMetadata(key, data):
		    cursor.execute('''INSERT INTO metadata( key, data)
		        VALUES(:key,:data)''',
		        { 'key':key.encode('utf-8'), 'data':data.encode('utf-8')})
		    db.commit()

    except IOError as e:
        db = sqlite3.connect(name+'_progress.sqlite')
        cursor = db.cursor()
        cursor.execute('''
            CREATE TABLE modules(id INTEGER PRIMARY KEY, name TEXT)
        ''')
        cursor.execute('''
            CREATE TABLE pages( itemkey TEXT, itemval TEXT, url TEXT, downloaded BOOLEAN)
        ''')
        cursor.execute('''
            CREATE TABLE metadata( key TEXT PRIMARY KEY, data TEXT)
        ''')
        cursor.execute('''
            CREATE UNIQUE INDEX idx_pages_url ON pages (url);
        ''')
        db.commit()

        def insertMetadata(key, data):
		    cursor.execute('''INSERT INTO metadata( key, data)
		        VALUES(:key,:data)''',
		        { 'key':key.encode('utf-8'), 'data':data.encode('utf-8')})
		    db.commit()

        insertMetadata("lastStarted", datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))
        insertMetadata("lastUpdated", datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))

    return db

def getItem(itemkey, itemval, url, cursor):
    cursor.execute('''SELECT downloaded from pages where url = ?''', (url,))
    downloaded = cursor.fetchone()[0]
    if downloaded is 0 :

        updateScreen( "getting {0}".format(url))
        getPage(url, ark_cookies)

        cursor.execute( '''update pages set downloaded = 1 where url = ?''', (url,))
        setLastUpdate()

def getItems(module, cursor):
        #describe_module = '{}api.php?req=getItems&itemkey={}'.format(root_url, module['itemkey'])

        #items = getJson(describe_module, ark_cookies)

        items = module['items']

        if len(items)>0:
            for item in items:
                itemkey = module['itemkey']
                itemval = item[module['itemkey']]
                url = itemkey+'/'+itemval
                sys.stdout.write("\033[K")
                sys.stdout.write("\r")
                sys.stdout.flush()
                message = "checking %s" % url
                sys.stdout.write( message)
                cursor.execute('''SELECT count(*) from pages where url = ?''', (url,))
                count = cursor.fetchone()[0]
                if count is 0 :
                    cursor.execute('''INSERT INTO pages(itemkey, itemval, url, downloaded)
                        VALUES(:itemkey,:itemval,:url,:downloaded)''',
                        {'itemkey':itemkey.encode('utf-8'), 'itemval':itemval.encode('utf-8'), 'url':url.encode('utf-8'), 'downloaded':False})
                    db.commit()
                getItem(itemkey, itemval, url, cursor)

def checkComplete(url):
    cursor.execute('''SELECT count(*) from pages where url = ?''', (url,))
    count = cursor.fetchone()[0]
    setLastUpdate()
    
    if count is 0 :
        cursor.execute('''INSERT INTO pages( url, downloaded)
            VALUES(:url,:downloaded)''',
            { 'url':url.encode('utf-8'), 'downloaded':False})
        db.commit()
    else :
        cursor.execute('''SELECT downloaded from pages where url = ?''', (url,))
        downloaded = cursor.fetchone()[0]

        if downloaded:
            return True

    return False

      

def getPagination(pages_list):
    pages = pages_list[0].findAll('li')
    for page in pages:
        links = page.findAll('a')
        for a in links:
            page_no = a['href'].split("/")[-1]
            new_page_url = root_url.strip('/') + a['href'].replace(ark_root,"")
            cursor.execute('''SELECT count(*) from pages where url = ?''', (new_page_url,))
            count = cursor.fetchone()[0]
            if count is 0 :
                cursor.execute('''INSERT INTO pages(url, downloaded)
                    VALUES(:url,:downloaded)''',
                    { 'url':new_page_url.encode('utf-8'), 'downloaded':False})
                db.commit()
                #checkComplete(new_page_url)
                #getSearchPage(new_page_url)

def getSearchPage(url):

    if checkComplete(url):
        return True

    updateMetadata("numberOfPages",str(int(getMetadata("numberOfPages"))+1))

    updateScreen( "downloading search page %s" % url)

    r = requests.post(url, cookies=ark_cookies)

    soup = BS(r.text)

    try:
        pages_list = copy.deepcopy(soup.findAll("ul", attrs={'class':"pag_list"}))
    except:
        pages_list_orig = soup.findAll("ul", attrs={'class':"pag_list"})
        print "error on copy.deepcopy of:"
        print pages_list_orig
        return False

    tail = "search" + url.split("search")[1]
    depth = len(tail.split("/"))-1
    processed_soup = processPage(soup, depth, False)

    try:
        os.makedirs(destination + tail[:tail.rfind("/")])
        updateScreen( "trying to make dirs %s" % tail[:tail.rfind("/")])
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    filename = destination + tail +'.html'
    with open(filename, 'wb') as f:
        f.write(processed_soup)
    updateScreen( "wrote page to %s" % filename)
    cursor.execute( '''update pages set downloaded = 1 where url = ?''', (url,))
    db.commit()

    if(pages_list):
        getPagination(pages_list)

    cursor.execute('''SELECT count(*) from pages ''')

    row = cursor.fetchone()

    numPages = getMetadata("numberOfPages")

    print("")

    print(" page {0} of {1} {2}%".format(row[0], numPages, round((float(row[0])/float(numPages))*100,2) ))

def getKeySearchPages(item):
    directory = destination+"search/key/%s" % item['id']
    try:
        os.makedirs(directory)
        os.makedirs(directory+"/all")
        os.makedirs(directory+"/all/page")
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    url = root_url+"search/key/%s/all" % item['id']

    updateScreen( "getting searchpages starting at %s" % url)

    getSearchPage(url)

    url = root_url+"search/key/%s" % item['id']

    getSearchPage(url)

    cursor.execute( ''' select url from pages where downloaded = 0 and itemkey is NULL ''' )
    not_downloaded = cursor.fetchall()

    for page in not_downloaded:
        getSearchPage(page[0])

    return True
    
def confirmTrailingSlash(string):
    if string[-1] != "/":
        return string + "/"
    return string

# getting cookies
def startSession():

    #gets the password interactively from the commandline
    pswd = getpass.getpass('Password for {}:'.format(ark_user))
    
    #use requests to try and login and get arkcookies
    start_session_url = '{0}/user_home.php?handle={1}&passwd={2}'.format(root_url, ark_user, pswd)
    r = requests.get(start_session_url,verify=True)
    ark_cookies = r.cookies

    # if we got no cookies, the password was probably wrong try again
    if len(ark_cookies) ==0:
        print "Access Denied"
        return startSession()
    else:
        print "Access Granted"
    
    return ark_cookies

root_url = confirmTrailingSlash(sys.argv[1])
#if a url is  http://example.com/project/ark/subdirectory/, this will give "project/ark/subdirect/"
destination = confirmTrailingSlash(sys.argv[2])

ark_root = root_url.split("/")[-3]+"/"+root_url.split("/")[-2] + "/"

print "arkroot {0}".format(ark_root)

destination = destination+ark_root

try:
    os.makedirs(destination)
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

# get the auth stuff, start a session and get cookies
ark_user = sys.argv[3]
ark_cookies = startSession()

#set up our progess db so we can store starting and stopping
db = initProgressDb(destination)
cursor = db.cursor()

def insertMetadata(key, data):
    cursor.execute('''INSERT INTO metadata( key, data)
        VALUES(:key,:data)''',
        { 'key':key.encode('utf-8'), 'data':data.encode('utf-8')})
    db.commit()

def updateMetadata(key, data):
    cursor.execute('''Update metadata set data = :data where key = :key''',
        { 'key':key.encode('utf-8'), 'data':data.encode('utf-8')})
    db.commit()

def getMetadata(key):
    cursor.execute('''SELECT data from metadata where key = ?''', (key,))
    metadata = cursor.fetchone()[0]
    return metadata

def setLastUpdate():
    updateMetadata("lastUpdate", datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))

#get the modules in our ARK
describe_ark_url = '{0}api.php?req=describeItems'.format(root_url)
ark = getJson(describe_ark_url, ark_cookies)

#set up extra pages
pages = [
    "search",
    "search/key",
    "search/atr"
]
for page in pages:
    try:
        os.makedirs(destination+page)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

#get core pages
core_pages = [
    'data_view.php',
    'user_home.php',
    'index.php',
    'map_view.php',
    'micro_view.php'
]


numberOfItems = len(core_pages);
try:
    getMetadata("numberOfPages")
except:
    print "no metadata"
    insertMetadata("numberOfPages", str(numberOfItems))

for page in core_pages:
        getPage(page,ark_cookies)

for item in ark:
    if(item['itemkey']!='cor_cd'):
        describe_module = '{0}api.php?req=getItems&item_key={1}'.format(root_url, item['itemkey'])
        items_json = getJson(describe_module, ark_cookies)
        try:
            items = items_json[item['shortform']]
            numberOfItems += len(items)

            item['items']=items
        except KeyError as e:

            item['items']=[]


cursor.execute( ''' SELECT count(*) as count FROM pages where itemkey is NULL ''' )
numberOfKnownSearchPages = cursor.fetchone()[0]

updateMetadata("numberOfPages", str(numberOfItems+int(numberOfKnownSearchPages)))

updateMetadata("lastStarted", datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))

updateMetadata("lastUpdate",datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))
# loop over the modules

for item in ark:
    setLastUpdate()
    #igonre the core "module"
    if(item['itemkey']!='cor_cd'):
        #try to make a directory for each module, failing quietly if it already exists
        try:
            os.makedirs(destination+"%s" % item['itemkey'])
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        #get all the items for this module
        getItems(item, cursor)
        #get all the search pages for this module
        getKeySearchPages(item)

cursor.execute( ''' select url from pages where downloaded = 0 and itemkey is NULL ''' )
not_downloaded = cursor.fetchall()
while(len(not_downloaded)>0):

    for page in not_downloaded:
        getSearchPage(page[0])

    cursor.execute( ''' select url from pages where downloaded = 0 and itemkey is NULL ''' )
    not_downloaded = cursor.fetchall()

#insertMetadata("lastCompleted", datetime.datetime.now().strftime("%Y/%m/%d %H:%M"))

