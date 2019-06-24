from __future__ import absolute_import

import os
from datetime import datetime

project = os.environ.get('GOOGLE_CLOUD_PROJECT')
if not project:
  raise EnvironmentError('GOOGLE_CLOUD_PROJECT is not set')

# Try to get the author/username to create separate resources for users.
author = (
    os.environ.get('ghprbPullAuthorLogin')  # Jenkins stores the author name here.
    or os.environ.get('USER')               # Try Linix/Unix/Mac environment.
    or os.environ.get('USERNAME')           # Try Windows environment.
    or 'apache'                             # If nothing works, default to 'apache'.
)
timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

# Get (almost guaranteed) unique names for Google Cloud resources.
unique_name = 'beam-{}-{}'.format(author, timestamp)
storage_bucket = '{}-{}'.format(project, unique_name)
bigquery_dataset = unique_name.replace('-', '_')
