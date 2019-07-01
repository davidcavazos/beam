<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Build a pipeline

## Setup

Create the necessary input and output Pub/Sub topic.

```sh
gcloud pubsub topics create sample-inputs
gcloud pubsub topics create sample-outputs
```

## Count page visits

```sh
PROJECT=$(gcloud config get-value project)

python -m apache_beam.examples.build_a_pipeline.count_page_visits \
  --input-topic projects/$PROJECT/topics/sample-inputs \
  --output-topic projects/$PROJECT/topics/sample-outputs
```

## Top 10 pages

## Clean up

```sh
gcloud pubsub topics delete sample-inputs
gcloud pubsub topics delete sample-outputs
```
