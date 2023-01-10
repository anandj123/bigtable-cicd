#Import the modules
import datetime
import os
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
# import pyyaml module
import yaml
from yaml.loader import SafeLoader

path = "."
dir_list = os.listdir(path)
for file in dir_list: 
    if (file.startswith('bigtable_schema_') and file.endswith('.yaml')):
        with open(file) as f:
            data = yaml.load(f, Loader=SafeLoader)
            print(data['table']['name'])

            project_id = data['project_id']
            instance_id = data['instance_id']
            table_id = data['table']['name']

            # The client must be created with admin=True because it will create a
            # table.
            client = bigtable.Client(project=project_id, admin=True)
            instance = client.instance(instance_id)

            print("Creating the {} table.".format(table_id))
            table = instance.table(table_id)

            print("Creating column family cf1 with Max Version GC rule...")
            # Create a column family with GC policy : most recent N versions
            # Define the GC policy to retain only the most recent 2 versions
            print(data['table']['column_families'])
            column_families = {}
            for cf in data['table']['column_families']:
                print(cf['name'])
                max_version = int(cf['max_versions'])
                max_versions_rule = column_family.MaxVersionsGCRule(max_version)
                #column_families[cf['name']] = max_versions_rule
                #max_age_rule
                max_age = int(cf['max_age_rule'])
                max_age_rule_ = column_family.MaxAgeGCRule(max_age)
                column_families[cf['name']] = column_family.GCRuleIntersection([max_versions_rule, max_age_rule_])
                print(column_families[cf['name']])


            #column_family_id = data['table']['column_families']['name']
            if not table.exists():
                print("Table does not exist")
                table.create(column_families=column_families)
            else:
                print("Table {} already exists.".format(table_id))
            print('Done')       





