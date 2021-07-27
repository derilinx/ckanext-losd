from ckan.lib.base import BaseController
from ckanext.losd import views_model


class UpdateHomeOrganization(BaseController):

    def update_home_org(self, org_1, org_2, org_3, org_4):
        return views_model.update_home_org(org_1, org_2, org_3, org_4)
