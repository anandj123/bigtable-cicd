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
import yaml
from yaml.loader import SafeLoader
from google.cloud.bigtable import Client
from google.cloud.bigtable import enums

#Function to create app profiles
def create_app_profile():

    path = "."
    dir_list = os.listdir(path)
    for file in dir_list: 
        #Scan and open the files with the appropriate naming convention
        if (file.startswith('app_profile') and file.endswith('.yaml')):
            print("Scanning " + file + " file...")
            with open(file) as f:
                data = yaml.load(f, Loader=SafeLoader)

                #Collect all app profile properties and go through validation
                if data['project_id'] is None:
                    print('A project id must be provided.')
                    exit()
                project_id = data['project_id']

                if data['instance_id'] is None:
                    print('An instance id must be provided.')
                    exit()
                instance_id = data['instance_id']

                for ap in data['app_profiles']:
                    if ap['app_profile'] is None:
                        exit()
                    
                    if ap['app_profile']['name'] is None:
                        print('An app profile id must be provided.')
                        exit()
                    
                    app_profile_id = ap['app_profile']['name']

                    if ap['app_profile']['routing_policy'] is None:
                        print('A routing policy type must be provided.')
                        exit()
                    routing_policy_type = ap['app_profile']['routing_policy']
                    
                    #Define properties for multi-cluster routing
                    if routing_policy_type == 'multi-cluster':
                        routing_policy_type = enums.RoutingPolicyType.ANY
                        description = "multi-cluster routing"
                        cluster_id = None
                        allow_transactional_writes = None
                    #Define properties for single-cluster routing
                    else:
                        routing_policy_type = enums.RoutingPolicyType.SINGLE
                        description = "single-cluster routing"
                        cluster_id = ap['app_profile']['cluster_id']
                        if ap['app_profile']['cluster_id'] is None:
                            print('A cluster id must be provided.')
                            exit()
                        allow_transactional_writes = ap['app_profile']['single_row_transaction']
                        if ap['app_profile']['single_row_transaction'] is None:
                            print('Must specify if single row transactions are allowed.')
                            exit()

                    client = Client(project=project_id, admin=True)
                    instance = client.instance(instance_id)

                    #Create the app profile
                    app_profile = instance.app_profile(
                        app_profile_id=app_profile_id,
                        routing_policy_type=routing_policy_type,
                        description=description,
                        cluster_id=cluster_id,
                        allow_transactional_writes=allow_transactional_writes
                    )

                    if not app_profile.exists():
                        app_profile = app_profile.create(ignore_warnings=True)
                        print("App profile {} created.".format(app_profile_id))
                    else:
                        print("App profile {} already exists.".format(app_profile_id))

def create_table():
    path = "."
    dir_list = os.listdir(path)
    for file in dir_list: 
        #Scan and open the files with the appropriate naming convention
        if (file.startswith('bigtable_schema_') and file.endswith('.yaml')):
            print("Scanning " + file + " file...")
            with open(file) as f:
                data = yaml.load(f, Loader=SafeLoader)
                
                #Collect all table properties and go through validation
                if data['project_id'] is None:
                    print('A project id must be provided.')
                    exit()
                project_id = data['project_id']

                if data['instance_id'] is None:
                    print('An instance id must be provided.')
                    exit()
                instance_id = data['instance_id']

                for tb in data['tables']:
                     if tb['table'] is None:
                         exit()
                     table_id = tb['table']['name']
                     if tb['table']['name'] is None:
                        print('A table id must be provided.')
                        exit()

                     client = bigtable.Client(project=project_id, admin=True)
                     instance = client.instance(instance_id)
                     table = instance.table(table_id)

                     # Create a column family with GC policy : most recent N versions and max N days for age
                     column_families = {}
                     
                     if tb['table']['column_families'] is None:
                            print('Column families are not defined.')
                            exit()
                     else:
                        for cf in tb['table']['column_families']:
                            max_version = int(cf['max_versions_rule'])
                            max_versions_rule = column_family.MaxVersionsGCRule(max_version)
                            max_age = int(cf['max_age_rule'])
                            max_age_rule_ = column_family.MaxAgeGCRule(datetime.timedelta(days=max_age))
                            column_families[cf['name']] = column_family.GCRuleUnion(rules=[max_versions_rule, max_age_rule_])

                     if not table.exists():
                         table.create(column_families=column_families)
                         print("Table {} created.".format(table_id))
                     else:
                         print("Table {} already exists.".format(table_id))


if __name__ == "__main__":
    create_app_profile()
    create_table()
