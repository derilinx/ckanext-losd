from ckan import plugins as p
from ckanext.losd.views import converter


class LosdMixinPlugin(p.SingletonPlugin):
    p.implements(p.IBlueprint)

    # IBlueprint
    def get_blueprint(self):
        # blueprint for this extension
        return [converter]

