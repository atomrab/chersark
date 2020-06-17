import json
import requests
import sys
import threading
import os
import csv
import time
import getpass
import random
import copy
from Queue import Queue

#getJson wraps requests with some useful checks and returns parsed Json
def getJson(url, cookie = False, values = False, username=False, password=False):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    try:
        if (username and password and values):
            r = requests.post(url, data=values, auth=(username, password))
        elif (username and password):
            r = requests.get(url, auth=(username, password))
        elif (cookie and values):
            requrl ="{}?{}".format(url,"&".join(['{}={}'.format(k,v) for k,v in values.iteritems()]))
            r = requests.post(requrl, json=json.dumps(values), cookies=cookie,headers=headers)
        elif (values):
            r = requests.post(url, data=values)
        else:
            r = requests.get(url)
    except requests.ConnectionError:
        time.sleep(5)
        return getJson(url, cookie, values)
    
    try:
        return r.json()
    except ValueError, e:
        print "JSON ERROR on request: {} with {}".format(url,values)
        try:
            return json.loads(r.text.split("<pre></pre>",1)[1])
        except IndexError, e:
            time.sleep(5)
            print "got: {}".format(r.text)
            return getJson(url, cookie, values)
    return False

# getting cookies
def startSession():

    #gets the password interactively from the commandline
    pswd = getpass.getpass('Password for {}:'.format(ark_user))
    
    #use requests to try and login and get arkcookies
    start_session_url = '{0}/user_home.php?handle={1}&passwd={2}'.format(root_url, ark_user, pswd)
    r = requests.get(start_session_url)
    ark_cookies = r.cookies

    # if we got no cookies, the password was probably wrong try again
    if len(ark_cookies) ==0:
        print "Access Denied"
        return startSession()
    else:
        print "Access Granted"
    
    return ark_cookies
    

# get a JSON representation of an item on ARK
def getItem(module,item):
    
    global ark_cookies,api_url
    #set up the item - @id and @type are json LD things
    item['itemkey'] = module['itemkey']
    item["@id"]="{}/{}/{}".format(root_url, item['itemkey'], item[item['itemkey']])
    item["@type"]="{}/{}".format(root_url, item['itemkey'])
    
    #empty list for properties
    fields = []
    
    #loop the modules' available fields
    for fieldidx in module['fields']:
        
        field = module['fields'][fieldidx]
        
        #different classtypes need handled differently as per resfieldfunction 
        fieldFunc = resFieldFunction.get(field['dataclass'], lambda a,b: ["Invalid classtype {}".format(b['dataclass'])])
        fielddata = fieldFunc(item, field)
        
        # shouldn't get none, tell the user if we do
        if fielddata == None:
            print "got None in field: {} for item: {}".format(field, item)
        
        #fieldfunctions return lists of results as a field may have more than one value eg a person may hold more than one role
        fields = fields + fielddata
    
    #flatten the properties into the item to make it json-ld like, multiple values become lists
    for field in fields:
        fieldid=field.keys()[0]
        if fieldid in item.keys():
            try:
                item[fieldid].append(field[fieldid])
            except:
                item[fieldid] = [item[fieldid],field[fieldid]]
        else:
            item[fieldid] = field[fieldid]
    
    #let the user know whats going on
    print "retrieved {}/{}".format(item['itemkey'],item[item['itemkey']])
    sys.stdout.flush()
    
    return item

def getItems(module):
    
    global ark_cookies,api_url,samplelen
    
    print "samplelen: {}".format(samplelen)
    
    items = []
    itemkey = module['itemkey']
    
    #let the user know whats going on
    print "loading module {}".format(itemkey)

    #the ARK api returns lists of items like { 'cxt':[...] }
    itemsLists = getJson(api_url, ark_cookies, {'req':'getItems','item_key':itemkey})
    itemList = itemsLists[itemkey[:3]]
    
    # if the user specified a sample length, only get that may items of each module
    if itemList:
        sample = random.sample(itemList,min(samplelen,len(itemList)))
    else:
        sample = {}
    
    print "samplelen: {}".format(len(sample))
    
    #a Queue will let us run some synchronous threads to get the items
    requestItemsQ = Queue(len(sample))
    for item in sample:
        requestItemsQ.put(item)

    # this function will run in each thread, emptying the queue and calling getItem
    def itemsProcess():
        item = requestItemsQ.get()
        items.append(getItem(module, item))
        requestItemsQ.task_done()

    # Twenty seems like a reasonable number of threads
    numberofthreads = min(20,len(sample))
    for i in range(numberofthreads):
        t = threading.Thread(target=itemsProcess)
        t.daemon = True
        t.start()

    #rejoin the Queue
    requestItemsQ.join()

    #let the user kneo whats going on and return the module
    print "loaded module {}".format(itemkey)
    return {
        "@id":"{}/{}".format(root_url,module['itemkey']),
        "items":items,
        "description":module['description']
    }

#a routine for turning ARK api into a prop name and adding it to our vocab
def cleanPropName(name):
    name.replace("/","%3A").replace(":","%2F").lower()
    return name

def getAttributeType(attributetype):
    return getProp(attributetype)
    
def getProp(uri):
    uri = uri.replace("https","http")
    try:
        return props[uri]
    except KeyError:
        urijson = getJson("{}/json".format(uri), ark_cookies)
        for item in urijson[urijson.keys()[0]]:
            if u'http://purl.org/dc/terms/title' in item.keys():
                props[uri] = item[u'http://purl.org/dc/terms/title']['value']
                return cleanPropName(props[uri])

#spans are handled slightly differently as they go in both directions
def getSpanLabel(spanlabel):
    spanlabel = spanlabel.replace("https","http")
    try:
        return spanlabels[spanlabel]
    except KeyError:
        spanlabeljson = getJson("{}/json".format(spanlabel), ark_cookies)
        for item in spanlabeljson[spanlabel]:
                try:
                    spanlabels[spanlabel] = item[u'http://purl.org/dc/terms/title']['value']
                except KeyError:
                    continue
        return spanlabels[spanlabel]
        
#functions used by the dataclass specific functions
def resFrag(item, field):
    itemkey = item['itemkey']
    itemRequest = {
        'req':'getFrags',
        'item_key':itemkey,
        itemkey:item[itemkey],
        'dataclass':field['dataclass'],
        'classtype':field['classtype']
    }
    
    return getJson( api_url, ark_cookies, itemRequest)

def resFrags(item, field):
    return [resFrag(item, field)]
    
def resField(item, field):
    
    fieldtype = field['dataclass']
    
    fielduri = "{}/concept/{}type/{}".format(root_url, fieldtype, field['classtype'])
    
    fieldid = getProp(fielduri)
    
    rawdata = resFrag(item, field)
    
    data = []
    
    try:
        for field in rawdata[item[item['itemkey']]][0]:
            if field:
                fielddata = field[fieldtype]
                data.append({ fieldid: fielddata })
    except TypeError:
        data.append({ fieldid: None })

    return data

#here are the get functions for various dataclass
def getAttribute(attribute, boolean):
    attribute = attribute.replace("https","http")
    try:
        return attributes[attribute+str(boolean)]
    except KeyError:
        attributejson = getJson("{}/json".format(attribute), ark_cookies)
        
        for item in attributejson[attributejson.keys()[0]]:
            if u'http://purl.org/dc/terms/title' in item.keys():
                attrvalue = item[u'http://purl.org/dc/terms/title']['value']
            elif u'http://www.w3.org/2004/02/skos/core#inScheme' in item.keys():
                attrid = getAttributeType(item[u'http://www.w3.org/2004/02/skos/core#inScheme']['value'])
            else:
                continue
        
        if not boolean:
            attrvalue="!{}".format(attrvalue)
        
        attributeprop = attribute+"/"+str(boolean)
        
        attributes[attributeprop] = {attrid:attrvalue}
        props[attributeprop] = cleanPropName(attrvalue)
        
        return attributes[attributeprop]
    
def resXmiField(item, field):
    
    itemkey = item['itemkey']
    itemRequest = {
        'req':'getFields',
        'item_key':itemkey,
        itemkey:item[itemkey],
        'fields[]':field['field_id']
    }
    
    data = getJson( api_url, ark_cookies, itemRequest)
    
    returndata = []

    for xmilist in data:
        try:
            for xmis in xmilist:
                for xmi in xmis:
                    xmiuri = "{}/concept/xmi/{}".format(root_url, xmi['xmi_itemkey'][0:3])
                    xmiprop = getProp(xmiuri)
                    returndata.append({ xmiprop: xmi['xmi_itemvalue'] })
        except TypeError:
            continue
    
    return returndata
    
def resAttrField(item, field):
    
    rawdata = resFrag(item, field)
        
    data = rawdata[item[item['itemkey']]][0]
    
    fieldid = field['field_id'].replace('conf_field_','')
    
    returndata = []
    
    if not data:
        propid = getAttributeType("{}/concept/attributetype/{}".format(root_url, field['classtype']))
        return [ {propid:data} ]
    
    chainRequest = {
        'req':'getFrags',
        'item_key':'cor_tbl_number',
        'dataclass':'attribute',
        'classtype':'all'
    }
    
    for attr in data:
        attr_boolean = '1' == attr['boolean']
        attrid = attr['attribute']
        attrprop = "{}/concept/attribute/{}".format(root_url, attrid)
        attrobj = getAttribute(attrprop, attr_boolean)
        if attr['attached_frags']:
            chainRequest['cor_tbl_attribute'] = attr['id']
            chainData = getJson( api_url, ark_cookies, chainRequest)
            for chainDatum in chainData:
                try:
                    dataclasstype = '{}type'.format(chainDatum['dataclass'])
                    chain_attr_boolean = '1' == attr['boolean']
                    chainid = "{}/concept/{}/type/{}".format(root_url,dataclasstype,chainDatum[dataclasstype] )
                    chainAttr = getAttribute(chainid, chain_attr_boolean)
                    fielddata.append( chainAttr )
                except TypeError:
                    if chainData[0][0] != False:
                        print "error on chainData: {}".format(chainData)
                    continue
        returndata.append(attrobj)
    
    return returndata

def resModtypeField(item, field):
    fieldid = field['field_id'].replace('conf_field_','')
    typeuri = "{}/concept/{}".format(root_url, fieldid)
    
    props[typeuri] = fieldid
    
    modtype = '{}type'.format(item['itemkey'][0:3])
        
    return [{fieldid: item[modtype]}]

def resFileField(item, field):
    
    fileid = "{}/concept/filetype/{}".format(root_url, field['classtype'])
    
    prop = getProp(fileid)
    
    rawdata = resFrag(item, field)
    
    data = []
    
    try:
        for filedata in rawdata[item[item['itemkey']]][0]:
            if filedata:
                
                fileid = filedata['file']
                
                fileuri = "{}/concept/file/{}".format(root_url, fileid)
                
                filejson = getJson("{}/json".format(fileuri), ark_cookies)
                
                for item in filejson[filejson.keys()[0]]:
                    try:
                        fileuri = item["http://purl.org/dc/terms/identifier"]["value"]
                    except KeyError:
                        continue
                
                data.append({ prop: fileuri })
    except TypeError:
        data.append({ prop: None })

    return data

def resActionField(item, field):
    
    itemkey = item['itemkey']
    itemRequest = {
        'req':'getFields',
        'item_key':itemkey,
        itemkey:item[itemkey],
        'fields[]':field['field_id']
    }
    
    data = getJson( api_url, ark_cookies, itemRequest)
    
    returndata = []

    for actionlist in data:
        try:
            for action in actionlist:
                actionuri = "{}/concept/actiontype/{}".format(root_url, field['classtype'])
                actionprop = getProp(actionuri)
                returndata.append({ actionprop: "{}/{}/{}".format(root_url, action['actor_itemkey'], action['actor_itemvalue']) })
        except TypeError:
            continue
    
    return returndata

def resNumberField(item, field):
    
    fieldtype = field['dataclass']
    
    fielduri = "{}/concept/{}type/{}".format(root_url, fieldtype, field['classtype'])
    
    fieldid = getProp(fielduri)
    
    rawdata = resFrag(item, field)
    
    returndata = []
    
    chainRequest = {
        'req':'getFrags',
        'item_key':'cor_tbl_number',
        'dataclass':'attribute',
        'classtype':'all'
    }
    
    data = rawdata[item[item['itemkey']]][0]
        
    if not data:
        returndata.append({ fieldid: None })
        return returndata
        
    for field in data:
        fieldvalue = float(field[fieldtype])
        fielddata = {"value": fieldvalue}
        if field['attached_frags']:
            chainRequest['cor_tbl_number'] = field['id']
            rawChainData = getJson( api_url, ark_cookies, chainRequest)
            # try:
            chainData = rawChainData[field['id']][0]
            for chainDatum in chainData:
                chainid = "{}/concept/attribute/{}".format(root_url, chainDatum['attribute'])
                chain_attr_boolean = '1' == chainDatum['boolean']
                try:
                    chainitem = getAttribute(chainid, chain_attr_boolean)
                except:
                    print "error on getAttribute in chain for {}".format(chainid)
                    chainitem = {chainid:chain_attr_boolean}
                fielddata.update(chainitem)
        returndata.append({ fieldid: fielddata })

    return returndata

def resSpans(item, field):
    
    fieldtype = field['dataclass']
    field['classtype'] = 'all'
    
    rawdata = resFrag(item, field)
    
    returndata = []
    
    data = rawdata[item[item['itemkey']]][0]
    
    if not data:
        return returndata
    
    for field in data:
        if field:
            if field['end'] == item[item['itemkey']]:
                fieldid = "{}/concept/spanlabel/{}/2".format(root_url, field['spanlabel'])
                fielddata = "{}/{}/{}".format(root_url, item['itemkey'], field['beg'])
            else:
                fieldid = "{}/concept/spanlabel/{}/1".format(root_url, field['spanlabel'])
                fielddata = "{}/{}/{}".format(root_url, item['itemkey'], field['end'])
            fieldid = getSpanLabel(fieldid)
            returndata.append({ fieldid: fielddata })

    return returndata

#map dataclasses to functions
resFieldFunction = {
    'action': resActionField,
    'modtype': resModtypeField,
    'attribute': resAttrField,
    'xmi': resXmiField,
    'txt': resField,
    'number': resNumberField,
    'date': resField,
    'file': resFileField,
    'span': resSpans, 
}

#set up a few variables from the user
root_url = sys.argv[1]
api_url = "{}/api.php".format(root_url)

ark_user = sys.argv[2]
ark_cookies = startSession()

try :
    samplelen = int(sys.argv[3])
except:
    samplelen = float("inf")

#set up some items to fill as we go through the process
#TODO put these into a different datastore so we can keep track of progess and restart after failure
modules = {}
attributes = {}
spanlabels = {}
props = {}
data = {
    "@id": root_url
}

#we'll use this list of dud terms to clean fields if we need to use it
#jodrell = ['field_op_hidden','actors_elementclass','actors_grp','sf_nav_type','op_xmi_itemkey','module','add_validation','edt_validation','edt_variables','add_variables','aliasinfo','editable','hidden']

arkModules = getJson(api_url, ark_cookies, {"req":"describeItems"});
for module in arkModules:
    fields = getJson(api_url, ark_cookies, {"req":"describeFields","item_key":module['itemkey']});
    if module['itemkey'] in ['cor_cd','fil_cd']:
        continue

    for k, v in fields.items():
        if k in ['conf_field_{}'.format(module['itemkey']), 'conf_field_itemkey','conf_field_skos','conf_field_linkeddata']:
            del fields[k]

#this loop deleted dud terms for debugging
#     for fieldid in fields:
#         for term in jodrell:
#             try:
#                 del fields[fieldid][term]
#             except KeyError:
#                 continue

    #put it in the modules object
    modules[module['itemkey']]={
        'itemkey':module['itemkey'],
        'fields':fields,
        'description':module['description']
    }

#get a queue of all the modules
x = len(modules)
requestQ = Queue(x)
for module in modules:
    requestQ.put(modules[module])

#this is the process for threading the modules
def process():
    global data
    
    module = requestQ.get()
    data[module['itemkey']]=getItems(module)
    requestQ.task_done()

#set up a thread for each module
for i in range(x):
    t = threading.Thread(target=process)
    t.daemon = True
    t.start()

#at the end of the queue let the user know whats going on
requestQ.join()
print "writing {} modules".format(x)
sys.stdout.flush()

#flip the props, so the IRI's become the values and the propnames the keys
context = {v: k for k, v in props.iteritems()}
#this is the context
data["@context"] = context

#write it out to some files
with open("output/terms.json", "w+") as write_file:
    write_file.write(json.dumps(context, indent=2))

with open("output/data.json", "w+") as write_file:
    write_file.write(json.dumps(data, indent=2))