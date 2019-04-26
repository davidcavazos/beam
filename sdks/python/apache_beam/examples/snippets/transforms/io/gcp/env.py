from __future__ import absolute_import

import os
from datetime import datetime

project = os.environ.get('GOOGLE_CLOUD_PROJECT')
if not project:
  raise EnvironmentError('GOOGLE_CLOUD_PROJECT is not set')

# Try to get the author/username to create separate resources for users.
author = (
    os.environ.get('ghprbPullAuthorLogin')  # Jenkins stores the author name here.
    or os.environ.get('USER')               # Try *nix environment.
    or os.environ.get('USERNAME')           # Try Windows environment.
    or 'apache'                             # If nothing works, default to 'apache'.
)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Get (almost guaranteed) unique names for Google Cloud resources.
default_name = '{}_{}_{}'.format(project, author, timestamp).replace('-', '_')
storage_bucket = default_name
pubsub_topic = default_name
pubsub_subscription = default_name
