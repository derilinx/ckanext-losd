from ckan import plugins as p
import ckan.plugins.toolkit as toolkit
import logging
from ckanext.losd import helpers as h

log = logging.getLogger(__name__)


if toolkit.check_ckan_version(min_version='2.9.0'):
    from ckanext.losd.plugin.flask_plugin import LosdMixinPlugin
else:
    from ckanext.losd.plugin.pylons_plugin import LosdMixinPlugin


class LosdPlugin(LosdMixinPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.ITemplateHelpers)

    def update_config(self, config_):
        toolkit.add_template_directory(config_, '../templates')
        toolkit.add_public_directory(config_, '../public')
        toolkit.add_resource('../fanstatic', 'losd')

    def get_helpers(self):
        return {
            'supported_rdf_format': h.supported_rdf_format,
            'get_org_widget_data': h.get_org_widget_data,
            'get_applications_host': h.get_applications_host
        }
