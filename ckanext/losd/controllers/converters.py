from ckan.lib.base import BaseController, render
from ckan.plugins import toolkit
from ckanext.losd import views_model
import logging

log = logging.getLogger(__name__)


class CSVConverter(BaseController):

    def convertToCSV(self):
        result = views_model.convert_to_csv()
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=result.get('id', ''))

    def convertToRDF(self):
        result = views_model.convert_to_rdf()
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=result.get('id', ''))


class RDFConverter(BaseController):

    def convertToRDFJobs(self):
        result = views_model.convert_json_state_to_rdf()
        log.info(result)
        toolkit.redirect_to(controller='package', action='read', id=result.get('id', ''))

    def pushToRDFStore(self):
        result = views_model.push_to_rdf_store()
        log.info(result)
        resource_id = result.get('resource_id', '')
        package_id = result.get('package_id', '')
        toolkit.redirect_to(controller='package', action='resource_read', id=package_id, resource_id=resource_id)
