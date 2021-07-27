from ckan.plugins import toolkit
from ckan.common import c
import ckan.model as model
import ckan.lib.jobs as jobs
import ckan.lib.helpers as h
from ckanext.losd import jsonstatToRDF_conv as RdfConv
from pyjstat import pyjstat
import requests
from requests.auth import HTTPDigestAuth
import json
import os
import urllib2
import logging

log = logging.getLogger(__name__)
local_file_to_use_for_RDFStore = '/var/lib/ckan/storage/uploads/topushinRDFStore.ttl'


def remove_tmp_file(filename):
    if os.path.isfile(filename):
        os.remove(filename)


def temp_rdf_file_create(source_url):
    """
    Creates temp rdf file
    :param source_url:
    :return:
    """
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


def error_handler(func):
    """
    Error hadling validatorn error, not found, etc
    :param func:
    :return:
    """
    def handler(*args, **kwargs):
        result = {
                "success": "",
                "id": toolkit.request.params.get('pkg_id', u''),
                "error": ""
            }
        try:
            result = func(*args, **kwargs)
        except toolkit.ValidationError as e:
            log.error(e)
            result['error'] = e.error_dict.get('message', 'Validation Error')
        except toolkit.ObjectNotFound as e:
            log.error(e)
            result['error'] = e.error_dict.get('message', 'Not Found')
        except Exception as e:
            log.error(e)
            result['error'] = "Unknown error"

        if result.get('error', ''):
            h.flash_error(result.get('error', ''))
        else:
            h.flash_notice(result.get('success', ''))
        return result

    return handler


@error_handler
def convert_to_csv():
    """
    Convert rdf to csv
    :return:
    """

    context = {
        'model': model,
        'session': model.Session,
        'user': c.user
    }

    resource_id = toolkit.request.params.get('resource_id', u'')
    log.info(toolkit.request.params)

    if not resource_id:
        raise toolkit.ValidationError("No resource id given")

    # dataset = losd.action.package_show(id=pkg_id)
    resource_jsonstat = toolkit.get_action('resource_show')(context, {"id": resource_id})
    Source_URL = resource_jsonstat['url']

    # read from json-stat
    dataset_jsonStat = pyjstat.Dataset.read(Source_URL)

    # write to dataframe
    df = dataset_jsonStat.write('dataframe')
    filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.csv'
    df.to_csv(filename, sep=',', encoding='utf-8', index=False)

    toolkit.get_action('resource_create')(context, {
        "package_id": toolkit.request.params.get('pkg_id', u''),
        "format": "csv",
        "name": 'csv ' + resource_jsonstat.get('name', ''),
        "decription": 'CSV file converted from json-stat resource:' + resource_jsonstat.get('name', ''),
        "upload": open(filename)
    })
    os.remove(filename)
    return {
        "success": toolkit._('A new CSV resource has been created.'),
        "id": toolkit.request.params.get('pkg_id', u''),
        "error": ""
    }


@error_handler
def convert_to_rdf():

    context = {
        'model': model,
        'session': model.Session,
        'user': c.user
    }

    resource_id = toolkit.request.params.get('resource_id', u'')
    resource_csv = toolkit.get_action('resource_show')(context, {"id": resource_id})
    Source_URL = resource_csv['url']

    # read from juma
    jumaUser = toolkit.request.params.get('jumaUser', u'')
    jumaMappingID = toolkit.request.params.get('jumaMappingID', u'')
    juma_url = 'https://'+str(os.environ['HOST_JUMA'])+'/juma-api?user=' + jumaUser + '&map=' + jumaMappingID + '&source=' + Source_URL

    # write to dataframe
    filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.ttl'
    try:
        response = urllib2.urlopen(juma_url)
    except Exception as e:
        log.error(e)
        return {
            "error": toolkit._('Error while getting the data from url: {}'.format(juma_url)),
            "id": toolkit.request.params.get('pkg_id', u''),
            "success": ""
        }

    chunk_size = 16 * 1024
    # CSO: Fix csv to rdf error #501
    # https://github.com/derilinx/derilinx/issues/501
    incorrect_info = False
    with open(filename, 'wb') as f:
        while True:
            chunk = response.read(chunk_size)
            if chunk == 'Error: Unable to find mapping file, ensure mappingID and userID is correct!':
                incorrect_info = True
                break
            if not chunk:
                break
            f.write(chunk)

    if incorrect_info:
        return {
            "error": toolkit._('Error: Unable to find mapping file, ensure mappingID and userID is correct!'),
            "id": toolkit.request.params.get('pkg_id', u''),
            "success": ""
        }
    else:
        toolkit.get_action('resource_create')(context, {
            "package_id": toolkit.request.params.get('pkg_id', u''),
            "format": "rdf",
            "name": toolkit.request.params.get('newResourceName', u'') or 'rdf ' + resource_csv.get('name', ''),
            "description": 'RDF file converted using JUMA from CSV resource:' + resource_csv.get('name', ''),
            "upload": open(filename)
        })

    os.remove(filename)
    return {
        "success": toolkit._('A new RDF resource has been created.'),
        "id": toolkit.request.params.get('pkg_id', u''),
        "error": ""
    }


def convert_json_state_to_rdf():
    """
    This creates json-stat to rdf conversion as background jobs and appropriate message will appear.
    Main conversion module is in the file controllers -> jsonstatToRDF.py file
    """
    resource_id = toolkit.request.params.get('resource_id', u'')
    datasetid = toolkit.request.params.get('datasetId', u'')
    vocabulary_namespace = toolkit.request.params.get('VocabNmSpace', u'')
    data_namespace = toolkit.request.params.get('DataNmSpace', u'')
    pkg_id = toolkit.request.params.get('pkg_id', u'')

    job = jobs.enqueue(RdfConv.convertToRDF, [resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id])
    task_id = job.id

    return {
        "message": toolkit._('RDF file being created. Please visit the dataset page after few minutes. '
                             'If you dont see the RDF file after a while, please contact administrator '
                             'along with the Job id:'+task_id),
        "id": pkg_id
    }


@error_handler
def push_to_rdf_store():

    context = {
        'model': model,
        'session': model.Session,
        'user': c.user
    }

    resource_id = toolkit.request.params.get('resource_id', u'')
    resource_rdf = toolkit.get_action('resource_show')(context, {"id": resource_id})
    source_url = resource_rdf['url']
    rdfStoreURL = toolkit.request.params.get('storeURL', u'').strip()
    rdfStoreUser = toolkit.request.params.get('userName', u'').strip()
    rdfStorePass = toolkit.request.params.get('password', u'').strip()
    graphIRI = toolkit.request.params.get('graphIRI', u'').strip()
    pkg_id = toolkit.request.params.get('pkg_id', u'').strip()

    result = {
        "success": "",
        "error": "",
        "resource_id": resource_id,
        "package_id": pkg_id
    }

    # Create a file for a given resource url
    if not temp_rdf_file_create(source_url):
        raise SystemError

    filename = local_file_to_use_for_RDFStore
    push_url = rdfStoreURL + '/sparql-graph-crud-auth?graph-uri=' + graphIRI

    if not os.path.exists(filename):
        raise OSError

    try:
        response = requests.post(push_url, data=open(filename, 'r').read(),
                                 auth=HTTPDigestAuth(rdfStoreUser, rdfStorePass))
        status_code = str(response.status_code)

        if (status_code == '201') or (status_code == '200'):
            remove_tmp_file(filename)
            result['success'] = toolkit._('Push to RDF store is successful!')
            return result
        elif status_code == '401':
            remove_tmp_file(filename)
            result['error'] = toolkit._('Invalid username or password!')
            return result
        elif status_code == '500':
            remove_tmp_file(filename)
            result['error'] = toolkit._('Invalid RDF file or Invalid graph uri. '
                                        'Please validate RDF or Graph URI!. Graph URI must be of type http://**')
            return result
        else:
            remove_tmp_file(filename)
            result['error'] = toolkit._('Bad request! Please validate RDF file, username and password.')
            return result
    except requests.exceptions.HTTPError as http_error:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Invalid RDF file. Please check RDF syntax')
        return result
    except requests.exceptions.ConnectionError as connection_error:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Invalid RDF Store URL: Please verify Virtuoso RDF Store URL')
        return result
    except requests.exceptions.Timeout as time_out_error:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Error Connecting:' + str(time_out_error))
        return result
    except requests.exceptions.RequestException as unkown_error:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Unkown Error:' + str(unkown_error))
        return result
    except SystemError:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Please verify that RDF file (URL) exists.')
        return result
    except OSError:
        remove_tmp_file(filename)
        result['error'] = toolkit._('Temporary file already exists. Please contact system administrator.')
        return result


def update_home_org(org_1, org_2, org_3, org_4):
    """
    Update home page organization (Ajax call)
    :param org_1:
    :param org_2:
    :param org_3:
    :param org_4:
    :return:
    """
    filename = '/var/lib/ckan/storage/uploads/homeorg_widget_data.json'
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            data['org_1'] = org_1
            data['org_2'] = org_2
            data['org_3'] = org_3
            data['org_4'] = org_4
            f.close()
        with open(filename, "w+") as jsonFile:
            json.dump(data, jsonFile)
            jsonFile.close()
        return json.dumps({'status': 'success'})
    except Exception as e:
        log.error(e)
        return json.dumps({'status': 'failure',
                           'message': 'Please make sure the json file is on the losd public folder'})
