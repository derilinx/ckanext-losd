from ckan import plugins as p
from ckanext.losd.views import converter
import ckan.plugins.toolkit as toolkit


class LosdMixinPlugin(p.SingletonPlugin):
    p.implements(p.IBlueprint)

    def update_config(self, config_):
        toolkit.add_template_directory(config_, '../templates-2.9')
        toolkit.add_public_directory(config_, '../public')
        toolkit.add_resource('../fanstatic', 'losd')

    # IBlueprint
    def get_blueprint(self):
        # blueprint for this extension
        return [converter]

