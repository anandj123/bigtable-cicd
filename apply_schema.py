"""Demonstrates how to connect to Cloud Bigtable and run some basic operations.
Prerequisites:
- Create a Cloud Bigtable cluster.
  https://cloud.google.com/bigtable/docs/creating-cluster
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

#Import the modules
import datetime
import os
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
# import pyyaml module
import yaml
from yaml.loader import SafeLoader
from google.cloud.bigtable import Client
from google.cloud.bigtable import enums

def create_app_profile():

    path = "."
    dir_list = os.listdir(path)
    for file in dir_list: 
        if (file.startswith('app_profile') and file.endswith('.yaml')):
            print(file)
            with open(file) as f:
                data = yaml.load(f, Loader=SafeLoader)

                project_id = data['project_id']
                instance_id = data['instance_id']
                for ap in data['app_profiles']:
                    if ap['app_profile'] is None:
                        break
                    app_profile_id = ap['app_profile']['name']
                    print(app_profile_id)

                    client = Client(project=project_id, admin=True)
                    instance = client.instance(instance_id)
                    routing_policy_type = ap['app_profile']['routing_policy']
                    
                    if routing_policy_type == 'multi-cluster':
                        routing_policy_type = enums.RoutingPolicyType.ANY
                        description = "multi-cluster routing"
                        cluster_id = None
                        allow_transactional_writes = None
                    else:
                        routing_policy_type = enums.RoutingPolicyType.SINGLE
                        description = "single-cluster routing"
                        cluster_id = ap['app_profile']['cluster_id']
                        allow_transactional_writes = ap['app_profile']['single_row_transaction']
                    
                    print("Creating the {} app profile.".format(app_profile_id))
                    app_profile = instance.app_profile(
                        app_profile_id=app_profile_id,
                        routing_policy_type=routing_policy_type,
                        description=description,
                        cluster_id=cluster_id,
                        allow_transactional_writes=allow_transactional_writes
                    )

                    if not app_profile.exists():
                        print("App profile does not exist")
                        app_profile = app_profile.create(ignore_warnings=True)
                    else:
                        print("App profile {} already exists.".format(app_profile_id))
                    print('Done')  

def create_table():
    path = "."
    dir_list = os.listdir(path)
    for file in dir_list: 
        if (file.startswith('bigtable_schema_') and file.endswith('.yaml')):
            #processing statement
            print(file)
            with open(file) as f:
                data = yaml.load(f, Loader=SafeLoader)

                project_id = data['project_id']
                instance_id = data['instance_id']
                for tb in data['tables']:
                    if tb['table'] is None:
                        break
                    table_id = tb['table']['name']
                    #check for name

                    # The client must be created with admin=True because it will create a
                    # table.
                    client = bigtable.Client(project=project_id, admin=True)
                    instance = client.instance(instance_id)

                    print("Creating the {} table.".format(table_id))
                    table = instance.table(table_id)

                    #print("Creating column family cf1 with Max Version GC rule...")
                    # Create a column family with GC policy : most recent N versions
                    # Define the GC policy to retain only the most recent 2 versions
                    #check for cf
                    column_families = {}
                    for cf in tb['table']['column_families']:
                        max_version = int(cf['max_versions'])
                        max_versions_rule = column_family.MaxVersionsGCRule(max_version)
                        #column_families[cf['name']] = max_versions_rule
                        #max_age_rule
                        max_age = int(cf['max_age_rule'])
                        max_age_rule_ = column_family.MaxAgeGCRule(datetime.timedelta(days=max_age))
                        column_families[cf['name']] = column_family.GCRuleUnion(rules=[max_versions_rule, max_age_rule_])

                    if not table.exists():
                        print("Table does not exist")
                        table.create(column_families=column_families)
                    else:
                        print("Table {} already exists.".format(table_id))

def delete_app_profile():
    client = Client(admin=True)
    instance = client.instance('my-instance')
    app_profile = instance.app_profile('my_app_profile_2')
    app_profile.reload()

    app_profile.delete(ignore_warnings=True)

if __name__ == "__main__":
    create_app_profile()
    #create_table()
    #delete_app_profile()
