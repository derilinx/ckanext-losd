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
import json
import ckan.lib.jobs as jobs
import sys
import requests

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


class JsonStatConverter(BaseController):

    def convertToRDFJobs(self):

        """ This creates json-stat to rdf conversion as background jobs and appropriate message will appear."""

        print "***********************************RDF CONV*****************************"

        resource_id = request.params.get('resource_id', u'')
        datasetid = request.params.get('datasetId', u'')
        vocabulary_namespace = request.params.get('VocabNmSpace', u'')
        data_namespace = request.params.get('DataNmSpace', u'')
        pkg_id = request.params.get('pkg_id', u'')

        convertToRDF(resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id)

        #job = jobs.enqueue(convertToRDF, [resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id])
        #task_id = job.id
        #h.flash_notice(_('RDF being created and may take a while. please visit the dataset page after few minutes. If you dont see the RDF file after a while, please contact administrator along with the Job id: '+task_id))
        tk.redirect_to(controller='package', action='read', id=pkg_id)



    def pushToRDFStore(self):
        losd = LocalCKAN()
        try:
            resource_id = request.params.get('resource_id', u'')
            resource_rdf = losd.action.resource_show(id=resource_id)
            Source_URL = resource_rdf['url']
            print('\n\n\n\n\n\n')
            print
            "Verification of the resource"
            dlfile(Source_URL)
            print('\n\n\n\n\n\n')
            filename = local_file_to_use_for_RDFStore
            filesize = os.path.getsize(filename)
            print
            filesize
            print('\n\n\n\n\n\n')
            # read from juma
            rdfStoreURL = request.params.get('storeURL', u'')
            rdfStoreUser = request.params.get('userName', u'')
            rdfStorePass = request.params.get('password', u'')
            graphIRI = request.params.get('graphIRI', u'')
            push_url = rdfStoreURL + '/sparql-graph-crud-auth?graph-uri=' + graphIRI
            print('\n\n\n\n\n\n')
            print('URL to RDF store: ' + push_url)
            print('\n\n\n\n\n\n')
            if not os.path.exists(filename):
                print
                "Error: the file '%s' does not exist" % filename
                raise SystemExit
            c = pycurl.Curl()
            c.setopt(c.POST, True)
            print('user ===> ' + rdfStoreUser + ':' + rdfStorePass)
            # c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
            c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
            c.setopt(pycurl.USERPWD, rdfStoreUser + ':' + rdfStorePass)
            c.setopt(pycurl.URL, push_url)
            c.setopt(pycurl.UPLOAD, 1)
            file = open(filename)
            c.setopt(c.READDATA, file)
            c.setopt(pycurl.POSTFIELDSIZE, filesize)
            print('Setting parameters')
            # Two versions with the same semantics here, but the filereader version
            # is useful when you have to process the data which is read before returning
            # c.setopt(pycurl.HTTPPOST, [("file1", (c.FORM_FILE, filename))])
            # c.setopt(pycurl.READFUNCTION, open(filename, 'rb').read)
            print('\n\n\n\n\n\n')
            print('uploading')
            c.setopt(c.VERBOSE, True)
            c.perform()
            c.close()
            print('\n\n\n\n\n\n')
            print('end uploading')
            os.remove(filename)
            id = request.params.get('pkg_id', u'')
            resource_id = request.params.get('resource_id', u'')
            h.flash_notice(_('This resource has been pushed to RDF store successfully.'))
            tk.redirect_to(controller='package', action='resource_read',
                           id=id, resource_id=resource_id)
        except:
            id = request.params.get('pkg_id', u'')
            resource_id = request.params.get('resource_id', u'')
            # h.flash_error(_('Error in pushing this resource to RDF store successfully.'))
            tk.redirect_to(controller='package', action='resource_read',
                           id=id, resource_id=resource_id)

def namespace_vocabspace_validator(string):

    ''' To validate the the namespace and vocabulary space links if it dosent ends with "\" '''

    if list(string)[-1] != "/":
        string = string + "/"

    return string

def convertToRDF(resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id):

    losd = LocalCKAN()

    # Add "/" at the end of data_namespace if not present.
    vocabulary_namespace = namespace_vocabspace_validator(vocabulary_namespace)
    data_namespace = namespace_vocabspace_validator(data_namespace) + datasetid +"/"
    vocabulary_namespace_prefix = "csov"
    data_namespace_prefix = "csod"

    resource_jsonstat = losd.action.resource_show(id=resource_id)

    Source_URL = resource_jsonstat['url']

    filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.ttl'

    if os.path.isfile(filename):
        os.remove(filename)

    # read from json-stat
    json1 = json.loads(urllib2.urlopen(Source_URL).read())


    def conversion_for_old():
        losd = LocalCKAN()

        scheme = '@prefix qb: <http://purl.org/linked-data/cube#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix prov: <http://www.w3.org/ns/prov#> .\n@prefix dc: <http://purl.org/dc/elements/1.1/> .\n@prefix ' + vocabulary_namespace_prefix + ': <' + vocabulary_namespace + '> .\n@prefix ' + data_namespace_prefix + ': <' + data_namespace + '> .\n\n#SCHEME\n\n'
        code_list = '#CODELIST\n\n'
        observations = '#OBSERVATIONS\n\n'

        #####Generating Scheme#####
        n1 = len(json1['dataset']['dimension']['id'])

        ##Scheme: Individual terms
        for a in range(n1):
            this1 = json1['dataset']['dimension']['id'][a]
            this2 = this1.replace(" ", "_").lower()
            scheme += '' + vocabulary_namespace_prefix + ':' + this2 + ' a qb:ComponentProperty, qb:DimensionProperty ;\n\trdfs:label "' + this1 + '" ;\n\trdfs:range xsd:string .\n\n'

        scheme += '' + vocabulary_namespace_prefix + ':value a qb:ComponentProperty, qb:MeasureProperty ;\n\trdfs:label "value" ;\n\trdfs:range xsd:float .\n\n'

        ##Scheme: DSD
        dataset = json1['dataset']['label'].replace(" ", "_").lower()

        scheme += '' + data_namespace_prefix + ':' + dataset + '_dsd a qb:DataStructureDefinition ;\n\tqb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' + data_namespace_prefix + ':conceptscheme/measureType ; \n\t\t  qb:dimension qb:measureType ;\n\t\t  qb:order 1 \n\t] ;\n\tqb:component [ qb:measure ' + vocabulary_namespace_prefix + ':value ] ;\n\t'

        for a in range(n1):
            this1 = json1['dataset']['dimension']['id'][a]
            this2 = this1.replace(" ", "_").lower()
            scheme += 'qb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' + data_namespace_prefix + ':conceptscheme/' + this2 + ' ;\n\t\t  qb:dimension ' + vocabulary_namespace_prefix + ':' + this2 + ' ;\n\t\t  qb:order ' + str(
                a + 2) + ' \n\t\t] '
            if a == n1 - 1:
                scheme += '\n.\n\n'
            else:
                scheme += ';\n\t'

        ##Scheme: Dataset
        scheme += '' + data_namespace_prefix + ':' + dataset + '_dataset a qb:DataSet ;\n\tqb:structure ' + data_namespace_prefix + ':' + dataset + '_dsd ;\n\trdfs:label "' + \
                  json1['dataset']['label'] + '" ; \n\tprov:generatedAtTime "' + json1['dataset'][
                      'updated'] + '"^^xsd:dateTime ;\n\tdc:creator "' + json1['dataset']['source'] + '" .\n\n'

        #####Generating Codelist#####

        ##Codelist: Conceptscheme
        for a in range(n1):
            this1 = json1['dataset']['dimension']['id'][a]
            this2 = this1.replace(" ", "_").lower()
            code_list += '' + data_namespace_prefix + ':conceptscheme/' + this2 + ' a skos:ConceptScheme ;\n\t'

            keys = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['index'].keys()

            cnt = 0

            for k in keys:
                concept = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['label'][k]
                concept = concept.replace(" ", "_").lower()
                code_list += 'skos:member ' + data_namespace_prefix + ':concept/' + this2 + '/' + concept + ' '
                if cnt == len(keys) - 1:
                    code_list += '.\n\n'
                else:
                    code_list += ';\n\t'
                cnt += 1

        ##Codelist: Concepts
        for a in range(n1):
            this1 = json1['dataset']['dimension']['id'][a]
            this2 = this1.replace(" ", "_").lower()

            keys = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['index'].keys()

            for k in keys:
                concept = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['label'][k]
                concept2 = concept.replace(" ", "_").lower()
                code_list += '' + data_namespace_prefix + ':concept/' + this2 + '/' + concept2 + ' a skos:Concept ;\n\trdfs:label "' + concept + '" .\n\n'

        #####Generating Observations#####

        all_term = []
        labels = []

        for a in range(n1):
            keys = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['index'].keys()
            labels = []

            for k in keys:
                concept = json1['dataset']['dimension'][json1['dataset']['dimension']['id'][a]]['category']['label'][k]
                concept2 = concept.replace(" ", "_").lower()
                labels.append(concept2)

            all_term.append(labels)

        size = json1['dataset']['dimension']['size']
        total_size = 1
        tracker = []

        for s in size:
            tracker.append(0)
            total_size *= s

        track_size = len(tracker)

        ##Observations: creating each

        for t in range(total_size):
            observations += '[ a qb:Observation ;\n\tqb:dataSet ' + data_namespace_prefix + ':' + dataset + '_dataset ;\n\tqb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t'

            for a in range(n1):
                this1 = json1['dataset']['dimension']['id'][a]
                this2 = this1.replace(" ", "_").lower()
                observations += '' + vocabulary_namespace_prefix + ':' + this2 + ' '
                observations += '' + data_namespace_prefix + ':' + 'concept/' + this2 + '/' + all_term[a][
                    tracker[a]] + ' ;\n\t'

            tracker[track_size - 1] += 1

            for i in range(track_size - 1, -1, -1):
                if i != 0:
                    if tracker[i] > size[i] - 1:
                        tracker[i] = 0
                        tracker[i - 1] += 1
                else:
                    if tracker[i] > size[i] - 1:
                        tracker[i] = 0

            observations += 'qb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t' + vocabulary_namespace_prefix + ':value "' + str(
                json1['dataset']['value'][t]) + '"^^xsd:float\n] . \n\n'

        with open(filename, 'w') as out_file:
            out_file.write(scheme)
            out_file.write(code_list)
            out_file.write(observations)
            out_file.close()

        losd.action.resource_create(
            package_id=pkg_id,
            format='rdf',
            name='RDF ' + resource_jsonstat['name'],
            description='RDF file converted from Json-Stat resource: ' + resource_jsonstat['name'],
            upload=open(filename)
        )

        os.remove(filename)
        print("Successfully converted")


    def conversion_for_new():
        losd = LocalCKAN()

        scheme = '@prefix qb: <http://purl.org/linked-data/cube#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix prov: <http://www.w3.org/ns/prov#> .\n@prefix dc: <http://purl.org/dc/elements/1.1/> .\n@prefix ' + vocabulary_namespace_prefix + ': <' + vocabulary_namespace + '> .\n@prefix ' + data_namespace_prefix + ': <' + data_namespace + '> .\n\n#SCHEME\n\n'
        code_list = '#CODELIST\n\n'
        observations = '#OBSERVATIONS\n\n'

        #####Generating Scheme#####
        n1 = len(json1['id'])
        unit_index = json1['id'].index('Units')

        ##Scheme: Individual terms
        for a in range(n1):
            this1 = json1['id'][a]
            this2 = this1.replace(" ", "_").lower()
            scheme += '' + vocabulary_namespace_prefix + ':' + this2 + ' a qb:ComponentProperty, qb:DimensionProperty ;\n\trdfs:label "' + this1 + '" ;\n\trdfs:range xsd:string .\n\n'

        scheme += '' + vocabulary_namespace_prefix + ':value a qb:ComponentProperty, qb:MeasureProperty ;\n\trdfs:label "value" ;\n\trdfs:range xsd:float .\n\n'

        ##Scheme: DSD
        dataset = json1['label'].replace(" ", "_").lower()

        scheme += '' + data_namespace_prefix + ':' + dataset + '_dsd a qb:DataStructureDefinition ;\n\tqb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' + data_namespace_prefix + ':conceptscheme/measureType ; \n\t\t  qb:dimension qb:measureType ;\n\t\t  qb:order 1 \n\t] ;\n\tqb:component [ qb:measure ' + vocabulary_namespace_prefix + ':value ] ;\n\t'

        for a in range(n1):
            if json1['id'][a] != 'Units':
                this1 = json1['id'][a]
                this2 = this1.replace(" ", "_").lower()
                scheme += 'qb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' + data_namespace_prefix + ':conceptscheme/' + this2 + ' ;\n\t\t  qb:dimension ' + vocabulary_namespace_prefix + ':' + this2 + ' ;\n\t\t  qb:order ' + str(
                    a + 2) + ' \n\t\t] '
                if a == n1 - 1:
                    scheme += '\n.\n\n'
                else:
                    scheme += ';\n\t'

        ##Scheme: Dataset
        scheme += '' + data_namespace_prefix + ':' + dataset + '_dataset a qb:DataSet ;\n\tqb:structure ' + data_namespace_prefix + ':' + dataset + '_dsd ;\n\trdfs:label "' + \
                  json1['label'] + '" ; \n\tprov:generatedAtTime "' + json1[
                      'updated'] + '"^^xsd:dateTime ;\n\tdc:creator "' + json1['source'] + '" .\n\n'

        #####Generating Codelist#####

        ##Codelist: Conceptscheme
        for a in range(n1):
            if json1['id'][a] != 'Units':
                this1 = json1['id'][a]
                this2 = this1.replace(" ", "_").lower()
                code_list += '' + data_namespace_prefix + ':conceptscheme/' + this2 + ' a skos:ConceptScheme ;\n\t'

                keys = json1['dimension'][json1['id'][a]]['category']['index'].keys()

                cnt = 0

                for k in keys:
                    concept = json1['dimension'][json1['id'][a]]['category']['label'][str(k)]
                    concept = concept.replace(" ", "_").lower()
                    code_list += 'skos:member ' + data_namespace_prefix + ':concept/' + this2 + '/' + concept + ' '
                    if cnt == len(keys) - 1:
                        code_list += '.\n\n'
                    else:
                        code_list += ';\n\t'
                    cnt += 1

        ##Codelist: Concepts
        for a in range(n1):
            if json1['id'][a] != 'Units':
                this1 = json1['id'][a]
                this2 = this1.replace(" ", "_").lower()

                keys = json1['dimension'][json1['id'][a]]['category']['index'].keys()

                for k in keys:
                    concept = json1['dimension'][json1['id'][a]]['category']['label'][k]
                    concept2 = concept.replace(" ", "_").lower()
                    code_list += '' + data_namespace_prefix + ':concept/' + this2 + '/' + concept2 + ' a skos:Concept ;\n\trdfs:label "' + concept + '" .\n\n'

        #####Generating Observations#####

        all_term = []
        labels = []

        for a in range(n1):
            if json1['id'][a] != 'Units':
                keys = json1['dimension'][json1['id'][a]]['category']['index'].keys()
                labels = []

                for k in keys:
                    concept = json1['dimension'][json1['id'][a]]['category']['label'][str(k)]
                    concept2 = concept.replace(" ", "_").lower()
                    labels.append(concept2)

                all_term.append(labels)

        size = json1['size']
        del size[unit_index]
        total_size = 1
        tracker = []

        for s in size:
            tracker.append(0)
            total_size *= s

        track_size = len(tracker)

        ##Observations: creating each

        for t in range(total_size):
            observations += '[ a qb:Observation ;\n\tqb:dataSet ' + data_namespace_prefix + ':' + dataset + '_dataset ;\n\tqb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t'

            cnt_all = 0

            for a in range(n1):
                if json1['id'][a] != 'Units':
                    this1 = json1['id'][a]
                    this2 = this1.replace(" ", "_").lower()
                    observations += '' + vocabulary_namespace_prefix + ':' + this2 + ' '
                    observations += '' + data_namespace_prefix + ':' + 'concept/' + this2 + '/' + all_term[cnt_all][tracker[cnt_all]] + ' ;\n\t'
                    cnt_all += 1

            tracker[track_size - 1] += 1

            for i in range(track_size - 1, -1, -1):
                if i != 0:
                    if tracker[i] > size[i] - 1:
                        tracker[i] = 0
                        tracker[i - 1] += 1
                else:
                    if tracker[i] > size[i] - 1:
                        tracker[i] = 0

            observations += 'qb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t' + vocabulary_namespace_prefix + ':value "' + str(
                json1['value'][t]) + '"^^xsd:float\n] . \n\n'

        with open(filename, 'w') as out_file:
            out_file.write(scheme)
            out_file.write(code_list)
            out_file.write(observations)
            out_file.close()


        losd.action.resource_create(
            package_id=pkg_id,
            format='rdf',
            name='RDF ' + resource_jsonstat['name'],
            description='RDF file converted from Json-Stat resource: ' + resource_jsonstat['name'],
            upload=open(filename)
        )

        os.remove(filename)

        print("Successfully converted")

    if "version" in json1.keys():
        conversion_for_new()
    else:
        conversion_for_old()