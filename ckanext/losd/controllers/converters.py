from ckan.lib.base import BaseController, render
from ckanapi import LocalCKAN, NotFound, ValidationError
import uuid
from pyjstat import pyjstat
import ckan.plugins.toolkit as tk
import ckan.model as model
import ckan.logic as logic
import ckan.lib.helpers as h
import os
import urllib2
import pycurl

_ = tk._
c = tk.c
request = tk.request
render = tk.render
abort = tk.abort
redirect = tk.redirect_to
NotFound = tk.ObjectNotFound
ValidationError = tk.ValidationError
check_access = tk.check_access
get_action = tk.get_action
tuplize_dict = logic.tuplize_dict
clean_dict = logic.clean_dict
parse_params = logic.parse_params
NotAuthorized = tk.NotAuthorized

local_file_to_use_for_RDFStore = '/var/lib/ckan/storage/uploads/topushinRDFStore.ttl'

def dlfile(full_url):

    #create the url and the request
    req = urllib2.Request(full_url)

    # Open the url
    try:
        f = urllib2.urlopen(req)

        # Open our local file for writing
        local_file = open(local_file_to_use_for_RDFStore, "wb" )
        #Write to our local file
        local_file.write(f.read())
        local_file.close()

    #handle errors
    except urllib2.HTTPError, e:
        print "HTTP Error:",e.code , url
    except urllib2.URLError, e:
        print "URL Error:",e.reason , url



def get_content(url):
    """ loads a webpage into a string """
    src = ''

    req = urllib2.Request(url)

    try:
        response = urllib2.urlopen(req)
        chunk = True
        while chunk:
            chunk = response.read(1024)
            src += chunk
        response.close()
    except IOError:
        print 'can\'t open',url 
        return src

    return src

class CSVConverter(BaseController):


    def convertToCSV(self):
        losd = LocalCKAN()
        try:
            resource_id = request.params.get('resource_id', u'')

            print("\n\n\n\n\n\n\n")
            print(request.params)


            # dataset = losd.action.package_show(id=pkg_id)
            resource_jsonstat = losd.action.resource_show(id=resource_id)

            Source_URL = resource_jsonstat['url']

            # read from json-stat
            dataset_jsonStat = pyjstat.Dataset.read(Source_URL)

            # write to dataframe
            df = dataset_jsonStat.write('dataframe')
            filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.csv'
            df.to_csv(filename, sep=',', encoding='utf-8', index=False)

            losd.action.resource_create(
                package_id=request.params.get('pkg_id', u''),
                format='csv',
                name='csv ' + resource_jsonstat['name'],
                description='CSV file converted from json-stat resource:' + resource_jsonstat['name'],
                upload=open(filename)
            )
            os.remove(filename)
            id = request.params.get('pkg_id', u'')
            h.flash_notice(_('A new CSV resource has been created.'))
            tk.redirect_to(controller='package', action='read',
                    id=id)
        except NotFound:
            print ('not found')


    def convertToRDF(self):
        losd = LocalCKAN()
        try:
            resource_id = request.params.get('resource_id', u'')
            resource_csv = losd.action.resource_show(id=resource_id)
            Source_URL = resource_csv['url']
            print('\n\n\n\n\n\n')
            print (Source_URL)
            # read from juma 
            jumaUser = request.params.get('jumaUser', u'')
            jumaMappingID = request.params.get('jumaMappingID', u'')
            juma_url = 'http://losd.staging.derilinx.com:8889/juma-api?user='+jumaUser+'&map='+jumaMappingID+'&source=' + Source_URL
            print (juma_url)
            #dataset_rdf = get_content(juma_url)
            # write to dataframe
            filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.ttl'
            #file = open(filename ,'w+')
            response = urllib2.urlopen(juma_url)
            CHUNK = 16 * 1024
            with open(filename, 'wb') as f:
                while True:
                    chunk = response.read(CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)

            #file.write(dataset_rdf)
            losd.action.resource_create(
                package_id=request.params.get('pkg_id', u''),
                format='rdf',
                name=request.params.get('newResourceName', u'') or 'rdf ' + resource_csv['name'],
                description='RDF file converted using JUMA from CSV resource:' + resource_csv['name'],
                upload=open(filename)
            )
            os.remove(filename)
            id = request.params.get('pkg_id', u'')
            h.flash_notice(_('A new RDF resource has been created.'))
            tk.redirect_to(controller='package', action='read',
                       id=id)
        except NotFound:
            print('not found')


    def pushToRDFStore(self):
        losd = LocalCKAN()
        try:
            resource_id = request.params.get('resource_id', u'')
            resource_rdf = losd.action.resource_show(id=resource_id)
            Source_URL = resource_rdf['url']
            print('\n\n\n\n\n\n')
            print "Verification of the resource"
            dlfile(Source_URL)
            print('\n\n\n\n\n\n')
            filename = local_file_to_use_for_RDFStore
            filesize = os.path.getsize(filename)
            print filesize
            print('\n\n\n\n\n\n')
            # read from juma 
            rdfStoreURL = request.params.get('storeURL', u'')
            rdfStoreUser =  request.params.get('userName', u'')
            rdfStorePass =  request.params.get('password', u'')
            graphIRI =  request.params.get('graphIRI', u'')
            push_url = rdfStoreURL + '/sparql-graph-crud-auth?graph-uri='+graphIRI
            print('\n\n\n\n\n\n')
            print ('URL to RDF store: ' + push_url)
            print('\n\n\n\n\n\n')
            if not os.path.exists(filename):
                print "Error: the file '%s' does not exist" % filename
                raise SystemExit
            c = pycurl.Curl()
            c.setopt(c.POST, True)
            print ('user ===> '+ rdfStoreUser+':'+rdfStorePass)
            #c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
            c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
            c.setopt(pycurl.USERPWD, rdfStoreUser+':'+rdfStorePass)
            c.setopt(pycurl.URL, push_url)
            c.setopt(pycurl.UPLOAD, 1)
            file = open(filename)
            c.setopt(c.READDATA, file)
            c.setopt(pycurl.POSTFIELDSIZE, filesize)
            print ('Setting parameters')
            # Two versions with the same semantics here, but the filereader version
            # is useful when you have to process the data which is read before returning
            #c.setopt(pycurl.HTTPPOST, [("file1", (c.FORM_FILE, filename))])
            #c.setopt(pycurl.READFUNCTION, open(filename, 'rb').read)
            print('\n\n\n\n\n\n')
            print ('uploading')
            c.setopt(c.VERBOSE, True)
            c.perform()
            c.close()
            print('\n\n\n\n\n\n')
            print ('end uploading')
            os.remove(filename)
            id = request.params.get('pkg_id', u'')
            resource_id = request.params.get('resource_id', u'')
            h.flash_notice(_('This resource has been pushed to RDF store successfully.'))
            tk.redirect_to(controller='package', action='resource_read',
                       id=id, resource_id=resource_id)
        except:
            id = request.params.get('pkg_id', u'')
            resource_id = request.params.get('resource_id', u'')
            #h.flash_error(_('Error in pushing this resource to RDF store successfully.'))
            tk.redirect_to(controller='package', action='resource_read',
                       id=id, resource_id=resource_id)


