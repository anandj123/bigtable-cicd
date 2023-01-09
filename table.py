#Import the modules
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

project_id = "hca-demo-project-373816"
instance_id = "my-instance"
table_id = "sample2"

# The client must be created with admin=True because it will create a
# table.
client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)

print("Creating the {} table.".format(table_id))
table = instance.table(table_id)

print("Creating column family cf1 with Max Version GC rule...")
# Create a column family with GC policy : most recent N versions
# Define the GC policy to retain only the most recent 2 versions
max_versions_rule = column_family.MaxVersionsGCRule(2)
column_family_id = "cf1"
column_families = {column_family_id: max_versions_rule}
if not table.exists():
    print("Table doesnot exist")
    table.create(column_families=column_families)
else:
    print("Table {} already exists.".format(table_id))
print('Done')