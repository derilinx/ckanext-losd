from flask import Blueprint, make_response
from ckanext.losd import views_model
from ckan.plugins import toolkit

converter = Blueprint(
    'converter',
    __name__,
    url_prefix=u'/converter',
    url_defaults={u'package_type': u'dataset'}
)


def convert_to_csv_view(package_type, id):
    result = views_model.convert_to_csv()
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=result.get('id', ''))


def convert_to_rdf_view(package_type, id):
    result = views_model.convert_to_rdf()
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=result.get('id', ''))


def convert_json_state_to_rdf_view(package_type, id):
    result = views_model.convert_json_state_to_rdf()
    log.info(result)
    return toolkit.redirect_to('{}.read'.format(package_type), id=result.get('id', ''))


def push_to_rdf_store_view(package_type, id):
    result = views_model.push_to_rdf_store()
    log.info(result)
    resource_id = result.get('resource_id', '')
    package_id = result.get('package_id', '')
    return toolkit.redirect_to('resource.read', id=package_id, resource_id=resource_id)


def update_home_org_view(package_type, org_1, org_2, org_3, org_4):
    return views_model.update_home_org(org_1, org_2, org_3, org_4)


converter.add_url_rule(
    u'/converttocsv/<id>', view_func=convert_to_csv_view
)

converter.add_url_rule(
    u'/converttordf/<id>', view_func=convert_to_rdf_view
)
converter.add_url_rule(
    u'/pushtordfstore/<id>', view_func=push_to_rdf_store_view
)

converter.add_url_rule(
    u'/jsonstattordf/<id>', view_func=convert_json_state_to_rdf_view
)

converter.add_url_rule(
    u'/updatehomeorg/<org_1>/<org_2>/<org_3>/<org_4>', view_func=update_home_org_view
)
