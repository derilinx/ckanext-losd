from ckan.lib.base import BaseController, render
from ckan.plugins import toolkit
from ckanext.losd import views_model
import logging

log = logging.getLogger(__name__)


class CSVConverter(BaseController):

    def convertToCSV(self, id, resource_id):
        result = views_model.convert_to_csv(id, resource_id)
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=id)

    def convertToRDF(self, id, resource_id):
        result = views_model.convert_to_rdf(id, resource_id)
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=result.get('id', ''))


class RDFConverter(BaseController):

    def convertToRDFJobs(self, id, resource_id):
        result = views_model.convert_json_state_to_rdf(id, resource_id)
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=id)

    def pushToRDFStore(self, id, resource_id):
        result = views_model.push_to_rdf_store(id, resource_id)
        log.info(result)
        toolkit.redirect_to(controller='package', action='resource_read', id=id, resource_id=resource_id)
