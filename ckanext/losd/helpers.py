import os
import json

SUPPORTED_RDF_FORMATS = ['rdf','ttl','n3','rdf/xml','rdf+xml']


def supported_rdf_format(format):
    return format.lower() in SUPPORTED_RDF_FORMATS


def get_org_widget_data():
    filename = '/var/lib/ckan/storage/uploads/homeorg_widget_data.json'
    try:
        if not os.path.isfile(filename):
            with open(filename, 'w') as f:
                json.dump({'org_1': 'na', 'org_2': 'na', 'org_3': 'na', 'org_4': 'na'}, f)
                f.close()
        with open(filename, 'r') as f:
            data = json.load(f)
            f.close()
        return data
    except Exception as e:
        return {'status': 'failure', 'message': 'Please make sure the json file is on the losd public folder'}


def get_applications_host(host_nm):
    return str(os.environ['SITE_SCHEME'])+str(os.environ[host_nm]).strip()
