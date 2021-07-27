from flask import Blueprint, make_response
from ckanext.losd import views_model
from ckan.plugins import toolkit
import logging

log = logging.getLogger(__name__)

converter = Blueprint(
    'converter',
    __name__,
    url_prefix=u'/converter',
    url_defaults={u'package_type': u'dataset'}
)


def convert_to_csv_view(package_type, id, resource_id):
    result = views_model.convert_to_csv(id, resource_id)
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=id)


def convert_to_rdf_view(package_type, id, resource_id):
    result = views_model.convert_to_rdf(id, resource_id)
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=id)


def convert_json_state_to_rdf_view(package_type, id, resource_id):
    result = views_model.convert_json_state_to_rdf(id, resource_id)
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=id)


def push_to_rdf_store_view(package_type, id, resource_id):
    result = views_model.push_to_rdf_store(id, resource_id)
    log.info(result)
    return toolkit.redirect_to('resource.read', id=id, resource_id=resource_id)


def update_home_org_view(package_type, org_1, org_2, org_3, org_4):
    return views_model.update_home_org(org_1, org_2, org_3, org_4)


converter.add_url_rule(
    u'/converttocsv/<id>/<resource_id>', view_func=convert_to_csv_view, methods=['POST']
)

converter.add_url_rule(
    u'/converttordf/<id>/<resource_id>', view_func=convert_to_rdf_view, methods=['POST']
)
converter.add_url_rule(
    u'/pushtordfstore/<id>/<resource_id>', view_func=push_to_rdf_store_view, methods=['POST']
)

converter.add_url_rule(
    u'/jsonstattordf/<id>/<resource_id>', view_func=convert_json_state_to_rdf_view, methods=['POST']
)

converter.add_url_rule(
    u'/updatehomeorg/<org_1>/<org_2>/<org_3>/<org_4>', view_func=update_home_org_view, methods=['POST']
)
