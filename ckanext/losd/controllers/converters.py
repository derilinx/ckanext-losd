from ckan.lib.base import BaseController, render
from ckanapi import LocalCKAN, NotFound, ValidationError
import uuid
from pyjstat import pyjstat
import ckan.plugins.toolkit as tk
import ckan.logic as logic
import ckan.lib.helpers as h
import os
import urllib2
import pycurl
import ckan.lib.jobs as jobs
from . import jsonstatToRDF_conv as RdfConv
import requests
from requests.auth import HTTPDigestAuth
import json

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


def temp_rdf_file_create(source_url):
    # create the url and the request

    # Load the rdf file from the resource url

    try:

        source_rdf = urllib2.urlopen(source_url).read()
        with open(local_file_to_use_for_RDFStore, "wb") as local_file:
            local_file.write(source_rdf)
            local_file.close()

        # Loading the rdf file to temporary path is successful
        return True

    except Exception as e:

        # False represents something wrong in opening a rdf resource or url
        return False


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
        print
        'can\'t open', url
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

            print('not found')

    def convertToRDF(self):

        losd = LocalCKAN()

        try:
            resource_id = request.params.get('resource_id', u'')
            resource_csv = losd.action.resource_show(id=resource_id)
            Source_URL = resource_csv['url']

            # read from juma
            jumaUser = request.params.get('jumaUser', u'')
            jumaMappingID = request.params.get('jumaMappingID', u'')
            juma_url = 'http://losd.staging.derilinx.com:8889/juma-api?user=' + jumaUser + '&map=' + jumaMappingID + '&source=' + Source_URL

            # dataset_rdf = get_content(juma_url)
            # write to dataframe
            filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.ttl'
            # file = open(filename ,'w+')
            try:
                response = urllib2.urlopen(juma_url)
            except Exception as e:

                id = request.params.get('pkg_id', u'')
                h.flash_error(_(e))
                tk.redirect_to(controller='package', action='read', id=id)

            CHUNK = 16 * 1024

            # CSO: Fix csv to rdf error #501
            # https://github.com/derilinx/derilinx/issues/501
            incorrect_info = False

            with open(filename, 'wb') as f:
                while True:
                    chunk = response.read(CHUNK)
                    if chunk == 'Error: Unable to find mapping file, ensure mappingID and userID is correct!':
                        incorrect_info = True
                        break
                    if not chunk:
                        break
                    f.write(chunk)

            # file.write(dataset_rdf)
            if incorrect_info:
                id = request.params.get('pkg_id', u'')
                h.flash_notice(_('Error: Unable to find mapping file, ensure mappingID and userID is correct!'))
                tk.redirect_to(controller='package', action='read', id=id)
            else:
                losd.action.resource_create(
                    package_id=request.params.get('pkg_id', u''),
                    format='rdf',
                    name=request.params.get('newResourceName', u'') or 'rdf ' + resource_csv['name'],
                    description='RDF file converted using JUMA from CSV resource:' + resource_csv['name'],
                    upload=open(filename)
                )
                id = request.params.get('pkg_id', u'')
                h.flash_notice(_('A new RDF resource has been created.'))
                tk.redirect_to(controller='package', action='read',
                               id=id)
            os.remove(filename)

        except NotFound:

            id = request.params.get('pkg_id', u'')
            h.flash_error(_('Something went wrong!.'))
            tk.redirect_to(controller='package', action='read',
                           id=id)


class RDFConverter(BaseController):

    def remove_tmp_file(self, filename):

        if os.path.isfile(filename):

            os.remove(filename)

    def convertToRDFJobs(self):

        """ This creates json-stat to rdf conversion as background jobs and appropriate message will appear.
        Main conversion module is in the file controllers -> jsonstatToRDF.py file """

        resource_id = request.params.get('resource_id', u'')
        datasetid = request.params.get('datasetId', u'')
        vocabulary_namespace = request.params.get('VocabNmSpace', u'')
        data_namespace = request.params.get('DataNmSpace', u'')
        pkg_id = request.params.get('pkg_id', u'')

        job = jobs.enqueue(RdfConv.convertToRDF, [resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id])
        task_id = job.id

        #res = RdfConv.convertToRDF(resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id)

        h.flash_notice(_('RDF file being created. Please visit the dataset page after few minutes. '
                         'If you dont see the RDF file after a while, please contact administrator '
                         'along with the Job id:'+task_id))
        tk.redirect_to(controller='package', action='read', id=pkg_id)

    def pushToRDFStore(self):

        losd = LocalCKAN()

        resource_id = request.params.get('resource_id', u'')
        resource_rdf = losd.action.resource_show(id=resource_id)
        source_url = resource_rdf['url']
        rdfStoreURL = request.params.get('storeURL', u'')
        rdfStoreUser = request.params.get('userName', u'')
        rdfStorePass = request.params.get('password', u'')
        graphIRI = request.params.get('graphIRI', u'')
        pkg_id = request.params.get('pkg_id', u'')

        # Create a file for a given resource url
        if not temp_rdf_file_create(source_url):
            raise SystemError

        filename = local_file_to_use_for_RDFStore
        push_url = rdfStoreURL + '/sparql-graph-crud-auth?graph-uri=' + graphIRI

        if not os.path.exists(filename):
            raise FileExistsError

        try:
            # This is equivalent curl command
            curl_result = os.popen('curl -X PUT/POST --digest -u "'+rdfStoreUser+':'+rdfStorePass+'" --url "'+ push_url
                      + '" -T ' + filename).read()

            if curl_result:
                self.remove_tmp_file(filename)
                h.flash_error(_('RDF syntax error, please check the RDF file.'))
                tk.redirect_to(controller='package', action='resource_read',
                               id=pkg_id, resource_id=resource_id)
                sys.exit(1)

            self.remove_tmp_file(filename)
            h.flash_notice(_('Please Note: No error response will be generated for invalid username and password. '
                             'Please check the RDF store for the successful push'))
            tk.redirect_to(controller='package', action='resource_read',
                           id=pkg_id, resource_id=resource_id)
            sys.exit(0)

        except FileNotFoundError:

            self.remove_tmp_file(filename)
            h.flash_error(_('Please verify that RDF file (URL) exists.'))
            tk.redirect_to(controller='package', action='resource_read', id=pkg_id, resource_id=resource_id)

        except FileExistsError:

            self.remove_tmp_file(filename)
            h.flash_error(_('Temporary file already exists. Please contact system administrator.'))
            tk.redirect_to(controller='package', action='resource_read', id=pkg_id, resource_id=resource_id)

