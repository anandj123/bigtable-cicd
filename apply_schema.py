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

    #client = bigtable.Client(
    project_id='hca-demo-project-373816'
    #client = bigtable.Client(project=project_id, admin=True)
    #instance = client.instance('my-instance')
    #table = bigtable.table.Table(table_id, instance, '[APP_PROFILE_ID]')
    #app_profile_id = "my-app-profile"
    #app_profile = instance.app_profile(app_profile_id)
    #app_profile.create(ignore_warnings=True,single_cluster_routing=instance.AppProfile.SINGLE_CLUSTER_ROUTING_ON)

    routing_policy_type = enums.RoutingPolicyType.ANY

    client = Client(project=project_id, admin=True)
    instance = client.instance('my-instance')

    description = "routing policy-multy"

    app_profile = instance.app_profile(
        app_profile_id='my-app-profile',
        routing_policy_type=routing_policy_type,
        description=description,
        cluster_id='my-instance-c1',
    )

    app_profile = app_profile.create(ignore_warnings=True)

    


def create_table():
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
                    max_age_rule_ = column_family.MaxAgeGCRule(datetime.timedelta(days=max_age))
                    column_families[cf['name']] = column_family.GCRuleUnion(rules=[max_versions_rule, max_age_rule_])


                #column_family_id = data['table']['column_families']['name']
                if not table.exists():
                    print("Table does not exist")
                    table.create(column_families=column_families)
                else:
                    print("Table {} already exists.".format(table_id))
                print('Done')       

create_app_profile()
#create_table()


