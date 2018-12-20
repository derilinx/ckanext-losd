from ckanapi import LocalCKAN, NotFound, ValidationError
import urllib2
import json
from tempfile import NamedTemporaryFile
import string
import re
import uuid


def _cleanString(s):

    """ For cleaning the space - replace space by _"""

    s = str(re.sub(r'\([^)]*\)', '', s.encode('utf-8')))
    s = s.translate(None, string.punctuation)

    return s.strip().replace(" ", "_").lower()


def prefix_build_concept(data_namespace_prefix, data_field_nm):
    return data_namespace_prefix + _cleanString(data_field_nm) + 'cpt:'


def urlize(*args):

    """
    Url builder for codel ist concept and scheme
    """

    return "csod" + "/".join(map(_cleanString, args))


def namespace_vocabspace_validator(string):

    """
    To validate the the namespace and vocabulary space links if it dosent ends with "/"
    """

    if list(string)[-1] != "/":
        string = string + "/"

    return string


def convertToRDF(resource_id, datasetid, vocabulary_namespace, data_namespace, pkg_id):

    """ This is used to call a specific function based on the version of json stat source."""

    losd = LocalCKAN()

    job_result = {}

    # Add "/" at the end of data_namespace if not present.
    vocabulary_namespace = namespace_vocabspace_validator(vocabulary_namespace)
    data_namespace = namespace_vocabspace_validator(data_namespace) + datasetid + "/"
    # Vocabulary prefix
    vocabulary_namespace_prefix = "losdv"
    # Data namespace prefix
    data_namespace_prefix = "losdd"

    resource_jsonstat = losd.action.resource_show(id=resource_id)
    source_url = resource_jsonstat['url']

    # read from json-stat from a url
    source_json = json.loads(urllib2.urlopen(source_url).read())

    def conversion_for_old_jstat_version():

        losd = LocalCKAN()


        scheme = [
            '@prefix qb: <http://purl.org/linked-data/cube#> .'
            '\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .'
            '\n@prefix skos: <http://www.w3.org/2004/02/skos/core#> .'
            '\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .'
            '\n@prefix prov: <http://www.w3.org/ns/prov#> .'
            '\n@prefix dc: <http://purl.org/dc/elements/1.1/> .'
            '\n@prefix ' + vocabulary_namespace_prefix + ': <' + vocabulary_namespace + '> .\n@prefix '
            + data_namespace_prefix + ': <' + data_namespace + '> .',
            '\n@prefix ' + data_namespace_prefix + 'schm: <' + data_namespace + 'conceptscheme/> .']

        code_list = ['#CODELIST\n\n']
        observations = ['#OBSERVATIONS\n\n']

        dataset_label = source_json['dataset']['label']
        dataset_source = source_json['dataset']['source']
        dataset_updated = source_json['dataset']['updated']
        dimensions = source_json['dataset']['dimension']

        # Building prefix

        for data_field_nm in dimensions['id']:
            scheme.append('\n@prefix ' + data_namespace_prefix + _cleanString(
                data_field_nm) + 'cpt: <' + data_namespace + 'concept/' + _cleanString(data_field_nm) + '/> .')

        scheme.append('\n\n#SCHEME\n\n')
        dataset_values = source_json['dataset']['value']
        n1 = len(dimensions['id'])

        # Scheme: Individual terms

        try:

            for data_field_nm in dimensions['id']:
                scheme.append('' + vocabulary_namespace_prefix + ':' + _cleanString(
                    data_field_nm) + ' a qb:ComponentProperty, qb:DimensionProperty ;\n\trdfs:label "' + data_field_nm.encode(
                    'utf-8') + '" ;\n\trdfs:range xsd:string .\n\n')

            scheme.append(
                '' + vocabulary_namespace_prefix + ':value a qb:ComponentProperty, qb:MeasureProperty ;\n\trdfs:label "value" ;\n\trdfs:range xsd:float .\n\n')

            # Scheme: DSD

            scheme.append('' + data_namespace_prefix + ':' + _cleanString(
                dataset_label) + '_dsd a qb:DataStructureDefinition ;\n\tqb:component\n\t\t'
                                 '[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' +
                          data_namespace_prefix + 'schm:measureType ; \n\t\t  qb:dimension qb:measureType ;'
                                                  '\n\t\t  qb:order 1 \n\t] ;\n\tqb:component [ qb:measure ' +
                          vocabulary_namespace_prefix + ':value ] ;\n\t')

            for index, data_field_nm in enumerate(dimensions['id']):
                scheme.append(
                    'qb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' + data_namespace_prefix +
                    'schm:' + _cleanString(
                        data_field_nm) + ' ;\n\t\t  qb:dimension ' + vocabulary_namespace_prefix + ':' + _cleanString(
                        data_field_nm) + ' ;\n\t\t  qb:order ' + str(index + 2) + ' \n\t\t] ')

                if index == (n1 - 1):
                    scheme.append('\n.\n\n')
                else:
                    scheme.append(';\n\t')

            # Scheme: Dataset

            scheme.append('' + data_namespace_prefix + ':' + _cleanString(dataset_label) +
                          '_dataset a qb:DataSet ;\n\tqb:structure ' + data_namespace_prefix + ':' +
                          _cleanString(dataset_label) + '_dsd ;\n\trdfs:label "' + \
                          dataset_label.encode('utf-8') + '" ; \n\tprov:generatedAtTime "' + dataset_updated
                          + '"^^xsd:dateTime ;\n\tdc:creator "' + dataset_source + '" .\n\n')

            # Generating Codelist

            # Codelist: Conceptscheme

            for data_field_nm in dimensions['id']:
                code_list.append('' + data_namespace_prefix + 'schm:' +
                                 _cleanString(data_field_nm) + ' a skos:ConceptScheme ;\n\t')

                skos_members = []
                for concept in dimensions[data_field_nm]['category']['label'].values():
                    skos_members.append(
                        'skos:member ' + prefix_build_concept(data_namespace_prefix, data_field_nm) + _cleanString(concept) + ' ')

                code_list.append(';\n\t'.join(skos_members) + '.\n\n')

            # Codelist: Concepts

            for data_field_nm in dimensions['id']:
                for concept in dimensions[data_field_nm]['category']['label'].values():
                    code_list.append('' + prefix_build_concept(data_namespace_prefix, data_field_nm) + _cleanString(concept) +
                                     ' a skos:Concept ;\n\trdfs:label "' + concept.encode('utf-8') + '" .\n\n')

            # Generating Observations

            all_term = []

            for data_field_nm in dimensions['id']:
                labels = []
                for concept in dimensions[data_field_nm]['category']['label'].values():
                    labels.append(_cleanString(concept))

                all_term.append(labels)

            size = dimensions['size']
            total_size = 1
            tracker = []

            for s in size:
                tracker.append(0)
                total_size *= s

            track_size = len(tracker)

            # Observations: creating each

            for t in xrange(total_size):
                observations.append(data_namespace_prefix + ':' + str(
                    uuid.uuid4()) + ' a qb:Observation ;\n\tqb:dataSet ' + data_namespace_prefix + ':' +
                                    _cleanString(dataset_label) + '_dataset ;\n\tqb:measureType ' +
                                    vocabulary_namespace_prefix + ':value ;\n\t')

                for index, data_field_nm in enumerate(dimensions['id']):
                    observations.append('' + vocabulary_namespace_prefix + ':' + _cleanString(data_field_nm) + ' ')
                    observations.append(
                        '' + prefix_build_concept(data_namespace_prefix, data_field_nm) + all_term[index][tracker[index]] + ' ;\n\t')

                tracker[track_size - 1] += 1

                for i in xrange(track_size - 1, -1, -1):
                    if i != 0:
                        if tracker[i] > size[i] - 1:
                            tracker[i] = 0
                            tracker[i - 1] += 1
                    else:
                        if tracker[i] > size[i] - 1:
                            tracker[i] = 0

                observations.append('qb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t' +
                                    vocabulary_namespace_prefix + ':value "' +
                                    str(dataset_values[t]) + '"^^xsd:float\n . \n\n')

            turtle_file = NamedTemporaryFile(suffix='.ttl', delete=True)

            with open(turtle_file.name, 'w') as out_file:
                out_file.write(''.join(scheme))
                out_file.write(''.join(code_list))
                out_file.write(''.join(observations))

            try:

                losd.action.resource_create(
                    package_id=pkg_id,
                    format='rdf',
                    name='RDF ' + str(datasetid),
                    description='RDF file converted from Json-Stat resource: ' + resource_jsonstat['name'],
                    upload=open(turtle_file.name)
                )

                turtle_file.file.close()

            except Exception as e:

                job_result['status'] = "Failed"
                job_result['Error'] = str(e)
                job_result['version'] = "old"
                job_result['Message'] = "Something went while uploading the rdf file"

                return job_result

        except Exception as e:

            job_result['status'] = "Failed"
            job_result['Error'] = str(e)
            job_result['version'] = "old"
            job_result['Message'] = "Something went wrong in the parsing jsonstat to rdf"

            return job_result

        job_result['status'] = "Success"
        job_result['Error'] = "None"
        job_result['version'] = "old"
        job_result['Message'] = "RDF file is successfully uploaded"

        return job_result

    def conversion_for_new_jstat_version():

        losd = LocalCKAN()

        scheme = [
            '@prefix qb: <http://purl.org/linked-data/cube#> .'
            '\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .'
            '\n@prefix skos: <http://www.w3.org/2004/02/skos/core#> .'
            '\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .'
            '\n@prefix prov: <http://www.w3.org/ns/prov#> .'
            '\n@prefix dc: <http://purl.org/dc/elements/1.1/> .'
            '\n@prefix ' + vocabulary_namespace_prefix + ': <' + vocabulary_namespace + '> .\n@prefix '
            + data_namespace_prefix + ': <' + data_namespace + '> .',
            '\n@prefix ' + data_namespace_prefix + 'schm: <' + data_namespace + 'conceptscheme/> .']

        code_list = ['#CODELIST\n\n']
        observations = ['#OBSERVATIONS\n\n']


        dataset_label = source_json['label']
        dataset_source = source_json['source']
        dataset_updated = source_json['updated']
        dimensions = source_json['dimension']
        dataset_values = source_json['value']
        field_nms = source_json['id']

        #### Building prefix

        for data_field_nm in field_nms:
            scheme.append('\n@prefix ' + data_namespace_prefix + _cleanString(
                data_field_nm) + 'cpt: <' + data_namespace + 'concept/' + _cleanString(data_field_nm) + '/> .')

        scheme.append('\n\n#SCHEME\n\n')

        # Generating Scheme

        unit_index = source_json['id'].index('Units')
        n1 = len(field_nms)

        try:

            # Scheme: Individual terms

            for data_field_nm in field_nms:
                scheme.append('' + vocabulary_namespace_prefix + ':' + _cleanString(
                    data_field_nm) + ' a qb:ComponentProperty, qb:DimensionProperty ;\n\trdfs:label "' + data_field_nm.encode(
                    'utf-8') + '" ;\n\trdfs:range xsd:string .\n\n')

                scheme.append(
                    '' + vocabulary_namespace_prefix + ':value a qb:ComponentProperty, qb:MeasureProperty ;'
                                                       '\n\trdfs:label "value" ;\n\trdfs:range xsd:float .\n\n')

            # Scheme: DSD

            scheme.append('' + data_namespace_prefix + ':' + _cleanString(
                dataset_label) + '_dsd a qb:DataStructureDefinition ;\n\tqb:component\n\t\t'
                                 '[ a qb:ComponentSpecification ;\n\t\t  qb:codeList ' +
                         data_namespace_prefix + 'schm:measureType ; \n\t\t  qb:dimension qb:measureType ;'
                                                 '\n\t\t  qb:order 1 \n\t] ;\n\tqb:component [ qb:measure ' +
                         vocabulary_namespace_prefix + ':value ] ;\n\t')

            for index, data_field_nm in enumerate(field_nms):
                if data_field_nm != 'Units':
                    scheme.append('qb:component\n\t\t[ a qb:ComponentSpecification ;\n\t\t  qb:codeList '
                                  + data_namespace_prefix + 'schm:' + _cleanString(data_field_nm) +
                                  ' ;\n\t\t  qb:dimension ' + vocabulary_namespace_prefix + ':' +
                                  _cleanString(data_field_nm) + ' ;\n\t\t  qb:order ' + str(index + 2) + ' \n\t\t] ')
                    if index == n1 - 1:
                        scheme.append('\n.\n\n')
                    else:
                        scheme.append(';\n\t')

            # Scheme: Dataset

            scheme.append('' + data_namespace_prefix + ':' + _cleanString(dataset_label) +
                          '_dataset a qb:DataSet ;\n\tqb:structure ' + data_namespace_prefix + ':' +
                          _cleanString(dataset_label) + '_dsd ;\n\trdfs:label "' + \
                          dataset_label.encode('utf-8') + '" ; \n\tprov:generatedAtTime "' + dataset_updated
                          + '"^^xsd:dateTime ;\n\tdc:creator "' + dataset_source + '" .\n\n')

            # Generating Code list

            # Code list: Conceptscheme

            for data_field_nm in field_nms:
                if data_field_nm != 'Units':
                    code_list.append('' + data_namespace_prefix + 'schm:' +
                                     _cleanString(data_field_nm) + ' a skos:ConceptScheme ;\n\t')

                    skos_members = []
                    for concept in dimensions[data_field_nm]['category']['label'].values():
                        skos_members.append(
                            'skos:member ' + prefix_build_concept(data_namespace_prefix, data_field_nm) +
                            _cleanString(concept) + ' ')

                    code_list.append(';\n\t'.join(skos_members) + '.\n\n')

            # Code list: Concepts

            for data_field_nm in field_nms:
                if data_field_nm != 'Units':
                    for concept in dimensions[data_field_nm]['category']['label'].values():
                        code_list.append('' + prefix_build_concept(data_namespace_prefix, data_field_nm) +
                                         _cleanString(concept) + ' a skos:Concept ;\n\trdfs:label "' +
                                         concept.encode('utf-8') + '" .\n\n')

            # Generating Observations

            all_term = []
            for data_field_nm in field_nms:
                if data_field_nm != 'Units':
                    labels = []
                    for concept in source_json['dimension'][data_field_nm]['category']['label'].values():
                        labels.append(_cleanString(concept))

                    all_term.append(labels)

            size = source_json['size']
            del size[unit_index]
            total_size = 1
            tracker = []

            for s in size:
                tracker.append(0)
                total_size *= s

            track_size = len(tracker)

            # Observations: creating each

            for t in xrange(total_size):
                observations.append(data_namespace_prefix + ':' + str(
                    uuid.uuid4()) + ' a qb:Observation ;\n\tqb:dataSet ' + data_namespace_prefix + ':' +
                                    _cleanString(dataset_label) + '_dataset ;\n\tqb:measureType ' +
                                    vocabulary_namespace_prefix + ':value ;\n\t')

                cnt_all = 0

                for data_field_nm in field_nms:

                    if data_field_nm != 'Units':

                        observations.append('' + vocabulary_namespace_prefix + ':'
                                            + _cleanString(data_field_nm) + ' ')
                        observations.append('' + prefix_build_concept(data_namespace_prefix, data_field_nm) +
                                            all_term[cnt_all][tracker[cnt_all]] + ' ;\n\t')
                        cnt_all += 1

                tracker[track_size - 1] += 1

                for i in xrange(track_size - 1, -1, -1):
                    if i != 0:
                        if tracker[i] > size[i] - 1:
                            tracker[i] = 0
                            tracker[i - 1] += 1
                    else:
                        if tracker[i] > size[i] - 1:
                            tracker[i] = 0

                observations.append('qb:measureType ' + vocabulary_namespace_prefix + ':value ;\n\t' +
                                    vocabulary_namespace_prefix + ':value "' +
                                    str(dataset_values[t]) + '"^^xsd:float\n . \n\n')

            turtle_file = NamedTemporaryFile(suffix='.ttl', delete=True)


            with open(turtle_file.name, 'w') as out_file:
                out_file.write("".join(scheme))
                out_file.write(''.join(code_list))
                out_file.write(''.join(observations))

            try:
                losd.action.resource_create(
                    package_id=pkg_id,
                    format='rdf',
                    name='RDF ' + str(datasetid),
                    description='RDF file converted from Json-Stat resource: ' + resource_jsonstat['name'],
                    upload=open(turtle_file.name)
                )

                turtle_file.file.close()

            except Exception as e:

                job_result['status'] = "Failed"
                job_result['Error'] = str(e)
                job_result['version'] = "New"
                job_result['Message'] = "Something went while uploading the rdf file"

                return job_result

        except Exception as e:

            job_result['status'] = "Failure"
            job_result['Error'] = str(e)
            job_result['version'] = "New"
            job_result['Message'] = "Something went wrong in parsing the json-stat to RDF"

            return job_result

        job_result['status'] = "Success"
        job_result['Error'] = "None"
        job_result['version'] = "New"
        job_result['Message'] = "RDF file is successfully uploaded"

        return job_result

    # Check for the version of the json-stat file

    if "version" in source_json.keys():

        conversion_for_new_jstat_version()

    else:

        conversion_for_old_jstat_version()


