from ckan.plugins import SingletonPlugin, implements, toolkit, IConfigurer, ITemplateHelpers, IRoutes
from ckanext.losd import helpers as h



class LosdPlugin(SingletonPlugin):
    implements(IConfigurer, inherit=True)
    implements(IRoutes, inherit=True)

    # IConfigurer

    implements(ITemplateHelpers)


    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'losd')

    def get_helpers(self):
        return {
            'supported_rdf_format': h.supported_rdf_format
        }

    def before_map(self, map):
        map.connect('converttocsv', '/dataset/converttocsv/{id}', controller='ckanext.losd.controllers.converters:CSVConverter', action='convertToCSV')
        map.connect('converttordf', '/dataset/converttordf/{id}', controller='ckanext.losd.controllers.converters:CSVConverter', action='convertToRDF')
        map.connect('pushtordfstore', '/dataset/pushtordfstore/{id}', controller='ckanext.losd.controllers.converters:CSVConverter', action='pushToRDFStore')
        map.connect('jsonstattordf', '/dataset/jsonstattordf/{id}', controller='ckanext.losd.controllers.converters:JsonStatConverter', action='convertToRDF')
        return map

    def after_map(self, map):
        return map
