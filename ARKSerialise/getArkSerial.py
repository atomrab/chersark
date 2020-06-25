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
    except requests.ConnectionError as r:
        sleepsec = random.random()*10
        print "Connection Error, retrying in {}s".format(round(sleepsec,2))
        sys.stdout.flush()
        time.sleep(sleepsec)
        return getJson(url, cookie, values)
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
        
        #fields = fields + fielddata
        
        #fieldfunctions return lists of results as a field may have more than one value eg a person may hold more than one role
        #flatten the properties into the item to make it json-ld like, multiple values become lists
        for data in fielddata:
            fieldid = data.keys()[0]
            try:
                if isinstance(item[fieldid], list):
                    item[fieldid].append( data[fieldid] )
                else:
                    item[fieldid] = [ item[fieldid], data[fieldid] ]
            except KeyError:
                item[fieldid] = data[fieldid]
#    #let the user know whats going on
#    print "retrieved {}/{}".format(item['itemkey'],item[item['itemkey']])
#    sys.stdout.flush()
    
    return item

def getItems(module):
    
    global ark_cookies,api_url,samplelen
        
    items = []
    itemkey = module['itemkey']
    
    #the ARK api returns lists of items like { 'cxt':[...] }
    itemsLists = getJson(api_url, ark_cookies, {'req':'getItems','item_key':itemkey})
    itemList = itemsLists[itemkey[:3]]
    
    # if the user specified a sample length, only get that may items of each module
    if itemList:
        sample = random.sample(itemList,min(samplelen,len(itemList)))
        # if (itemkey == "cxt_cd") :
#             sample.append({"itemkey":"cxt_cd","cxt_cd":"CH05SR_434"})
#
        # if (itemkey == "smp_cd") :
   #          sample.append({"smp_cd":"CH04SR_2","itemkey":"smp_cd"})
    else:
        sample = {}
        
    #let the user know whats going on
    print "loading {} {}s".format(len(sample),itemkey)
    sys.stdout.flush()
    
    #a Queue will let us run some synchronous threads to get the items
    requestItemsQ = Queue(len(sample))
    for item in sample:
        requestItemsQ.put(item)

    # this function will run in each thread, emptying the queue and calling getItem
    def itemsProcess():
        while True:
            item = requestItemsQ.get()
            items.append(getItem(module, item))
            requestItemsQ.task_done()
            print "{}% of {}s complete".format(int((float(len(items)) / float(len(sample)))*100) , itemkey)

    # Fifteen seems like a reasonable number of thread
    # after testing - more than that the VM starts to reject connections
    numberofthreads = min(15,len(sample))
    for i in range(numberofthreads):
        t = threading.Thread(target=itemsProcess)
        t.daemon = True
        t.start()
        time.sleep(0.5)

    #rejoin the Queue
    requestItemsQ.join()

    #let the user know whats going on and return the module
    return items

#a routine for turning ARK api into a prop name and adding it to our vocab
def cleanPropName(name):
    return name.replace("/","_").replace(":"," to ").lower()

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
                props[uri] = cleanPropName(item[u'http://purl.org/dc/terms/title']['value'])
                return props[uri]

#spans are handled slightly differently as they go in both directions
def getSpanLabel(spanlabel):
    spanlabel = spanlabel.replace("https","http")
    try:
        return spanlabels[spanlabel]
    except KeyError:
        spanlabeljson = getJson("{}/json".format(spanlabel), ark_cookies)
        for item in spanlabeljson[spanlabel]:
                try:
                    spanlabels[spanlabel] = item[u'http://purl.org/dc/terms/title']['value'].lower()
                except KeyError:
                    continue
        props[spanlabel] = spanlabels[spanlabel]
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
                attrvalue = item[u'http://purl.org/dc/terms/title']['value'].lower()
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
        'item_key':'cor_tbl_attribute',
        'dataclass':'number',
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
            for chainDatum in chainData[attr['id']][0]:
                try:
                    dataclasstype = '{}type'.format(chainDatum['dataclass'])
                    chainid = "{}/concept/{}/{}".format(root_url,dataclasstype,chainDatum[dataclasstype] )
                    chainprop = getProp(chainid)
                    chainval = chainDatum[chainDatum['dataclass']]
                    atpro=attrobj.keys()[0]
                    attrobj[atpro] = { atpro: attrobj[atpro], chainprop: chainval }
                except TypeError:
                    print "error on chainData: {}".format(chainData)
                    continue
        returndata.append(attrobj)
    
    return returndata

def resModtypeField(item, field):
    #the modtype is already in the object
    return []
    # fieldid = field['field_id'].replace('conf_field_','')
#     typeuri = "{}/concept/{}".format(root_url, fieldid)
#
#     props[typeuri] = fieldid
#
#     modtype = '{}type'.format(item['itemkey'][0:3])
#
#     return [{fieldid: item[modtype]}]

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
    
    returndata = []
    
    fieldtype = field['dataclass']
    
    fielduri = "{}/concept/{}type/{}".format(root_url, fieldtype, field['classtype'])
    
    fieldid = getProp(fielduri)
    
    rawdata = resFrag(item, field)
    
    data = rawdata[item[item['itemkey']]][0]
        
    if not data:
        returndata.append({ fieldid: None })
        return returndata
    
    chainRequest = {
        'req':'getFrags',
        'item_key':'cor_tbl_number',
        'dataclass':'attribute',
        'classtype':'all'
    }
        
    for field in data:
        fieldvalue = float(field[fieldtype])
        fielddata = {"value": fieldvalue}
        if field['attached_frags']:
            chainRequest['cor_tbl_number'] = field['id']
            rawChainData = getJson( api_url, ark_cookies, chainRequest)
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
    #fill the data object a lists of the items in that module
    data[module['itemkey'][:3]]=getItems(module)
    requestQ.task_done()

#set up a thread for each module
for i in range(x):
    t = threading.Thread(target=process)
    t.daemon = True
    t.start()
    time.sleep(1/float(x))

#at the end of the queue let the user know whats going on
requestQ.join()
print "resolving context"
sys.stdout.flush()

#flip the props, so the IRI's become the values and the propnames the keys
context = {v: k for k, v in props.iteritems()}

with open("output/rawcontext.json", "w+") as write_file:
    write_file.write(json.dumps(context, indent=2))

tot = len(context)

i=0
ont = []

for prop in context:
    irisplit = context[prop].split("/")
    if irisplit[-1] == "True":
        if "{}/False".format("/".join(irisplit[:-1])) not in props:
            context[prop] = "/".join(irisplit[:-1])
    ontobj={
        "@id":context[prop],
        "prefLabel":prop
    }
    i=i+1
    print "{}% complete".format(round((float(i)/float(tot))*100,2))
    conceptJson = getJson("{}/json".format(context[prop]),ark_cookies)
    concepts = conceptJson[conceptJson.keys()[0]]
    labels = []
    for concept in concepts:
        if u'closeMatch' in concept.keys():
            ontobj[u'closeMatch'] = concept[u'closeMatch']['value']
        if u'exactMatch' in concept.keys():
            ontobj[u'exactMatch'] = concept[u'exactMatch']['value']
        if u'http://www.w3.org/2004/02/skos/core#inScheme' in concept.keys():
            ontobj[u'inScheme'] = concept[u'http://www.w3.org/2004/02/skos/core#inScheme']['value']
        if u'http://www.w3.org/2001/XMLSchema#type' in concept.keys():
            ontobj[u'@type'] = concept[u'http://www.w3.org/2001/XMLSchema#type']['value']
        if u'http://www.w3.org/2004/02/skos/core#label' in concept.keys():
            label = concept[u'http://www.w3.org/2004/02/skos/core#label']
            labels.append({label["lang"]:label["value"]})
        if u'http://www.w3.org/2000/01/rdf-schema#label' in concept.keys():
            label = concept[u'http://www.w3.org/2000/01/rdf-schema#label']
            labels.append({label["lang"]:label["value"]})
    ontobj["label"] = labels
    ont.append(ontobj)

ontology= {
    "@context": {
        "label": { "@id": 'http://www.w3.org/2004/02/skos/core#label', "@container": "@language" },
        "closeMatch" : "http://www.w3.org/2004/02/skos/core#closeMatch",
        "exactMatch" : "http://www.w3.org/2004/02/skos/core#exactMatch",
        "inScheme" : "http://www.w3.org/2004/02/skos/core#inScheme"
    },
    "@set":ont
}

with open("output/ontology.json", "w+") as write_file:
    write_file.write(json.dumps(ontology, indent=2))

#add the module keys to the context
for module in modules.keys():
    modkey =module[:3] 
    context[modkey] = "{}/module/{}".format(root_url, modkey)
    
#this is the context
data["@context"] = context

print "writing {} modules".format(x)
with open("output/data.json", "w+") as write_file:
    write_file.write(json.dumps(data, indent=2))
