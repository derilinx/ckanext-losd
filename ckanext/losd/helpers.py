import os
import json

SUPPORTED_RDF_FORMATS = ['rdf','ttl','n3','rdf/xml','rdf+xml']


def supported_rdf_format(format):
    return format.lower() in SUPPORTED_RDF_FORMATS


def get_org_widget_data():

    filename = os.path.normpath(os.path.join(os.path.abspath(__file__), "../public/org_data/homeorg_widget_data.json"))

    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            f.close()

        return data

    except Exception as e:

        return {'status': 'failure', 'message': 'Please make sure the json file is on the losd public folder'}
