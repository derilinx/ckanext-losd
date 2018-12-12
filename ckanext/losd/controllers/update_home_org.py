from ckan.lib.base import BaseController, render
from ckanapi import LocalCKAN, NotFound, ValidationError
import uuid
from pyjstat import pyjstat
import ckan.plugins.toolkit as tk
import ckan.lib.base as base
import ckan.model as model
import ckan.logic as logic
import ckan.lib.helpers as h
import os
import urllib2
import pycurl
import json
import ckan.lib.jobs as jobs
import sys

_ = tk._
c = tk.c
request = tk.request
render = tk.render


class UpdateHomeOrganization(BaseController):

    def update_home_org(self, org_1, org_2, org_3, org_4):

        filename = os.path.join(__file__, "../../public/org_data/homeorg_widget_data.json")
        filename = os.path.normpath(filename)

        try:

            with open(filename, "r") as f:
                data = json.load(f)
                data['org_1'] = org_1
                data['org_2'] = org_2
                data['org_3'] = org_3
                data['org_4'] = org_4
                f.close()

            with open(filename, "w+") as jsonFile:
                json.dump(data, jsonFile)
                jsonFile.close()

            return json.dumps({'status': 'success'})

        except Exception as e:

            print(e)
            return json.dumps({'status': 'failure',
                               'message': 'Please make sure the json file is on the losd public folder'})
