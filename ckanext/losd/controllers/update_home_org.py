from ckan.lib.base import BaseController, render
import ckan.plugins.toolkit as tk
import os
import json


_ = tk._
c = tk.c
request = tk.request
render = tk.render


class UpdateHomeOrganization(BaseController):

    def update_home_org(self, org_1, org_2, org_3, org_4):

        filename = '/var/lib/ckan/storage/uploads/homeorg_widget_data.json'
<<<<<<< HEAD
	print("************************************************")
	print(filename)
	print(org_1, org_2, org_3, org_4)
=======
>>>>>>> e500a29bb08e919e6d69b94db9313670313fd7ac

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
