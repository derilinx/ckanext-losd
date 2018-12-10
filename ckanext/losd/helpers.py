

SUPPORTED_RDF_FORMATS = [
    'rdf','ttl','n3','rdf/xml','rdf+xml']

def supported_rdf_format(format):
    return format.lower() in SUPPORTED_RDF_FORMATS
