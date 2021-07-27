from ckan import plugins as p


class LosdMixinPlugin(p.SingletonPlugin):
    """
    Pylons plugins for ckan 8
    """

    def before_map(self, map):
        map.connect('converttocsv', '/converter/converttocsv/{id}',
                    controller='ckanext.losd.controllers.converters:CSVConverter', action='convertToCSV')
        map.connect('converttordf', '/converter/converttordf/{id}',
                    controller='ckanext.losd.controllers.converters:CSVConverter', action='convertToRDF')
        map.connect('pushtordfstore', '/converter/pushtordfstore/{id}',
                    controller='ckanext.losd.controllers.converters:RDFConverter', action='pushToRDFStore')
        map.connect('jsonstattordf', '/converter/jsonstattordf/{id}',
                    controller='ckanext.losd.controllers.converters:RDFConverter', action='convertToRDFJobs')
        map.connect('updatehomeorg', '/updatehomeorg/{org_1}/{org_2}/{org_3}/{org_4}',
                    controller='ckanext.losd.controllers.update_home_org:UpdateHomeOrganization',
                    action='update_home_org')

        return map

    def after_map(self, map):
        return map
