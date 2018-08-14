from ckan.lib.base import BaseController, render
from ckanapi import LocalCKAN, NotFound, ValidationError
import uuid
from pyjstat import pyjstat
import ckan.plugins.toolkit as tk
import ckan.model as model
import ckan.logic as logic
import ckan.lib.helpers as h
import os

_ = tk._
c = tk.c
request = tk.request
render = tk.render
abort = tk.abort
redirect = tk.redirect_to
NotFound = tk.ObjectNotFound
ValidationError = tk.ValidationError
check_access = tk.check_access
get_action = tk.get_action
tuplize_dict = logic.tuplize_dict
clean_dict = logic.clean_dict
parse_params = logic.parse_params
NotAuthorized = tk.NotAuthorized

class CSVConverter(BaseController):

    def convertToCSV(self):
        losd = LocalCKAN()
        try:
            resource_id = request.params.get('resource_id', u'')

            print("\n\n\n\n\n\n\n")
            print(request.params)


            # dataset = losd.action.package_show(id=pkg_id)
            resource_jsonstat = losd.action.resource_show(id=resource_id)

            Source_URL = resource_jsonstat['url']

            # read from json-stat
            dataset_jsonStat = pyjstat.Dataset.read(Source_URL)

            # write to dataframe
            df = dataset_jsonStat.write('dataframe')
            filename = '/var/lib/ckan/storage/uploads/' + unicode(uuid.uuid4()) + '.csv'
            df.to_csv(filename, sep=',', encoding='utf-8', index=False)

            losd.action.resource_create(
                package_id=request.params.get('pkg_id', u''),
                format='csv',
                name='csv ' + resource_jsonstat['name'],
                description='CSV file converted from json-stat resource:' + resource_jsonstat['name'],
                upload=open(filename)
            )
            os.remove(filename)
            id = request.params.get('pkg_id', u'')
            h.flash_notice(_('A new CSV resource has been created.'))
            tk.redirect_to(controller='package', action='read',
                    id=id)
        except NotFound:
            print ('not found')



