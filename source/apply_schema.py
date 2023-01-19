#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" 
Scans the current github repo directory for file names
    bigtable_schema_*.yaml
    app_profile_*.yaml
and apply those schema and app profile changes to the appropriate
BigTable instance
"""

import datetime
import os
from zipapp import create_archive
import yaml
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from yaml.loader import SafeLoader
from google.cloud.bigtable import Client
from google.cloud.bigtable import enums

''' 
Recursively go through each of the sub-directories and look for the 
file name prefix and suffix and call appropriate functions for the schema
changes.
'''
def scan_files():
    path = "."
    for r, d, f in os.walk(path):
        for file in f:
            if (file.startswith('app_profile_') and file.endswith('.yaml')):
                create_app_profile(os.path.join(r, file))
            elif (file.startswith('bigtable_schema_') and file.endswith('.yaml')):
                create_bigtable_table(os.path.join(r, file))

''' Function to create app profiles '''
def create_app_profile(file):
    print("Scanning " + file + " file...")
    with open(file) as f:
        data = yaml.load(f, Loader=SafeLoader)

        #Collect all app profile properties and go through validation
        if data['project_id'] is None:
            print('A project id must be provided.')
            return
        project_id = data['project_id']

        if data['instance_id'] is None:
            print('An instance id must be provided.')
            return
        instance_id = data['instance_id']

        for ap in data['app_profiles']:
            if ap['app_profile'] is None:
                continue
            
            if ap['app_profile']['name'] is None:
                print('An app profile id must be provided.')
                continue
            
            app_profile_id = ap['app_profile']['name']

            if ap['app_profile']['routing_policy'] is None:
                print('A routing policy type must be provided.')
                continue
            routing_policy_type = ap['app_profile']['routing_policy']
            
            #Define properties for multi-cluster routing
            if routing_policy_type == 'multi-cluster':
                routing_policy_type = enums.RoutingPolicyType.ANY
                description = "multi-cluster routing"
                cluster_id = None
                allow_transactional_writes = None
            #Define properties for single-cluster routing
            elif routing_policy_type == 'single-cluster':
                routing_policy_type = enums.RoutingPolicyType.SINGLE
                description = "single-cluster routing"
                cluster_id = ap['app_profile']['cluster_id']
                if ap['app_profile']['cluster_id'] is None:
                    print('A cluster id must be provided.')
                    continue
                allow_transactional_writes = ap['app_profile']['single_row_transaction']
                if ap['app_profile']['single_row_transaction'] is None:
                    print('Must specify if single row transactions are allowed.')
                    continue
            else: 
                continue

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

''' Function to create tables '''
def create_bigtable_table(file):
    print("Scanning " + file + " file...")
    with open(file) as f:
        data = yaml.load(f, Loader=SafeLoader)
        
        #Collect all table properties and go through validation
        if data['project_id'] is None:
            print('A project id must be provided.')
            return
        project_id = data['project_id']

        if data['instance_id'] is None:
            print('An instance id must be provided.')
            return
        instance_id = data['instance_id']

        for tb in data['tables']:
            if tb['table'] is None:
                continue
            table_id = tb['table']['name']
            if tb['table']['name'] is None:
                print('A table id must be provided.')
                continue

            client = bigtable.Client(project=project_id, admin=True)
            instance = client.instance(instance_id)
            table = instance.table(table_id)

            # Create a column family with GC policy : most recent N versions and max N days for age
            column_families = {}
            
            if tb['table']['column_families'] is None:
                print('Column families are not defined.')
                continue
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
    scan_files()
    create_app_profile(file)
    create_bigtable_table(file)
