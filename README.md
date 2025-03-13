# A Spark Streaming Pipeline Defined in YAML

This repo contains the result of a 4-day data-engineering hack to process records from Kafka using a Spark Structured Streaming pipeline, defined by a YAML specification, and docker compose to raise a small collection of containers. 
The demonstrator is of a pseudonymiser pipeline which takes patient records and applies a series of transforms to remove identifying details, outputting pseudonymised patient records, and an audit trail of the changes made. 
There are some  caveats, which I'll outline at the end. 

## Fixture creation

To feed the pipeline I create [fixtures/output/sample_data.json](fixtures/output/sample_data.json) containing 1000 randomly generated patients, which look like this:
```json
{"patient_id":0,"patient_name":"Roll Serum-Overskirt","city":"London","postcode":"BR5 2RE","disease":"Melanocytic nevus of skin"}
{"patient_id":1,"patient_name":"Redden Sternum-Misnomer","city":"Lancaster and Morecambe","postcode":"LA2 0NR","disease":"Diverticular disease of colon"}
{"patient_id":2,"patient_name":"Ethelene Lighterage-Speculativeness","city":"London","postcode":"N21 3PE","disease":"Chronic kidney disease stage 4"}
{"patient_id":3,"patient_name":"Lulah Conjunction-Afterburner","city":"London","postcode":"RM12 6RQ","disease":"Alzheimer's disease"}
{"patient_id":4,"patient_name":"Ivey Vote-Athleticism","city":"Wakefield and Castleford","postcode":"WF2 7FE","disease":"Morbid obesity"}
{"patient_id":5,"patient_name":"Crystal Dinner-Verticality","city":"Leeds","postcode":"LS11 0DS","disease":"Dystrophia unguium"}
{"patient_id":6,"patient_name":"Sybilla Quinsy-Cannabis","city":"Manchester","postcode":"WA15 6HX","disease":"Otomycosis"}
{"patient_id":7,"patient_name":"Metha Nitrate-Retribution","city":"Newcastle","postcode":"NE29 6JE","disease":"Upper respiratory infection"}
{"patient_id":8,"patient_name":"Lyndia Bight-Lap","city":"London","postcode":"W14 9UE","disease":"Cerebrovascular accident"}
{"patient_id":9,"patient_name":"Linna Repression-Grandson","city":"Dudley","postcode":"DY7 5NJ","disease":"Ulcerative colitis"}
{"patient_id":10,"patient_name":"Euphemia Fete-Envelope","city":"Newtown and Welshpool","postcode":"SY21 0EE","disease":"Diarrhea of presumed infectious origin"}
```
Fixture creation takes place in [src/fixtures.py](src/fixtures.py), and is driven by [fixture_config.json](fixture_config.json). 

To make fixture generation repeatable, the data source assets, defined in `download: [...]` are downloaded then transformed by the functions listed in `transform: [...]`. 
For example, the National Statistics Postcode Lookup data is downloaded by:
```json
{
    "info": "https://geoportal.statistics.gov.uk/datasets/525b74a332c84146add21c9b54f5ce68/about",
    "url": "https://www.arcgis.com/sharing/rest/content/items/525b74a332c84146add21c9b54f5ce68/data",
    "description": "National Statistics Postcode Lookup (Feb 2025)",
    "filepath": "./data/source/NSPL/NSPL_FEB_2025.zip"
}
```
Then processed in a number of transforms, including:
```json
{
    "function": "postcode_transform",
    "params": {
        "nspl_fact_table_filename": "./data/source/NSPL/Data/NSPL_FEB_2025_UK.csv",
        "ttwa_dimension_filename": "./data/source/NSPL/Documents/TTWA names and codes UK as at 12_11 v5.csv",
        "rgn20cd_dimension_filename": "./data/source/NSPL/Documents/Region names and codes EN as at 12_20 (RGN).csv",
        "city_table_output": "./data/output/postcodes_to_city.parquet",
        "region_table_output": "./data/output/city_to_region.parquet"
    }
}
```
Forenames are roughly the most unusual pre-1930s entries in the American census. 
Surnames are all double-barreled, formed from random nouns taken from the `wordnet` data packaged in Python `nltk`. 
It is always comforting when demonstrator data to looks distinctively un-real. 
Diagnoses are taken from NHS Digital Primary Care SNOMED code aggregate usage.
There frequency in this sample data match the frequency in the source dataset. 

## Pipeline Definition

Pipelines are defined in [src/metastore.yml](src/metastore.yml). 
The first section defines schemas for the various parts of the pipeline, for example:
```yaml
schemas:
  - name: patient_source
    primary_field: patient_id
    fields:
      patient_id: integer
      patient_name: string
      city: string
      postcode: string
      disease: string
```
The next section defines parameters for connection to the external services used by the pipeline, including Kafka and DeltaLake. 
A number of values are left for definition from environment variables, especially where values are the remit of devops 
(these are filled out in [docker-compose.yml](docker-compose.yml)).
```yaml
connectors:
  - id: kafka_dev
    connector_type: kafka
    broker_env: MESSAGE_BROKER
    topics:
    - topic_name: patient-source
      topic_schema: patient_source
  - id: deltalake_dev
    connector_type: deltalake
    tables:
      - table_name: patient_pseudo
        table_schema: patient_pseudo
        filepath_env: PATIENT_PSEUDO_FILEPATH
        partition_on: inserted_at
```
The metastore defines three pipelines, The first `patient_producer` is a small service which reads the `sample_data.json` fixture and outputs patients
one-by-one into a Kafka topic, at a randomised rate. 
```yaml
pipelines:
  - id: patient_producer
    pipeline_type: kafka_producer
    source_filepath_env: PRODUCER_SOURCE_FIXTURE
    sink_topic: kafka_dev.patient-source
    interval_seconds_mean: 0.5
    interval_seconds_sd: 1
```
The second pipeline `patient_pseudonymiser` connects to the kafka source, streams outputs to a deltalake table, and sends audit records to a file. 
```yaml
  - id: patient_pseudonymiser
    pipeline_type: transformer
    source: 
      source_type: kafka
      connector_id: kafka_dev
      topic: patient-source
    sink: 
      sink_type: deltalake
      connector_id: deltalake_dev
      table: patient_pseudo
      trigger_seconds: 60
      coalesce: 1
    audit: 
      sink_type: file
      filepath_env: PATIENT_PSEUDO_AUDIT_FILEPATH
      format: json
      trigger_seconds: 60
      coalesce: 1
    transforms:
      - transform_type: translate
        source_column: city
        destination_column: region
        fixture_filepath_env: TRANSLATE_CITY_REGION_FIXTURE
        key_column: city
        value_column: region
      - transform_type: redact
        source_column: patient_name
        destination_column: patient_name
        replace_with: XXXXX
      - transform_type: split_pick
        source_column: postcode
        destination_column: postcode
        split_by: ' '
        pick_index: 0
```
Three reusable transforms are applied. The first uses a lookup table to convert the `city` column into a `region` column. 
The second replaces the patient name with the string `XXXXX`.
The last removes the last section of the postcode ie from `BR5 2RE` to `BR5`. 

A final pipeline `memory_patient_pseudonymiser` is another version of the one above, but whose sinks are all in-memory. 
They are connected to an asynchronous Python generator, which yields dataframes that can be `display()`ed continuously within a notebook. 

## Pipeline Construction

The `metastore.yaml` file is validated using `Pydantic` models defined in [src/meta.py](src/meta.py).
Each model has an `infraclass` property, which defines the corresponding pipeline implementation code. For example, a meta-model instance `meta_instance` of class `meta.SplitPick`:
```python
class SplitPick(Transform):
    transform_type: Literal['split_pick']
    split_by: str
    pick_index: int
    infraclass: Type[transformation.SplitPickTransform] = transformation.SplitPickTransform
```
is implemented by calling `infra_instance = meta_instance.infraclass.bootstrap(meta_instance...)`, defined by the class `transformation.SplitPickTransform`:
```python
class SplitPickTransform(AuditingTransform, Infraclass):
    def __init__(self, transform_name, source_column, destination_column, primary_column, split_by, pick_index):
        tag_params = dict(split_by=split_by, pick_index=pick_index)
        super().__init__(transform_name, source_column, destination_column, primary_column, tag_params)
        self.split_by, self.pick_index = split_by, pick_index

    def apply_transform(self, stream_df, spark_session):
        split_col = SQL.split(stream_df[self.source_column], self.split_by)
        transformed_stream = stream_df.withColumn(self.destination_column, split_col.getItem(0))
        return transformed_stream
    
    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        primary_column = kwargs.get('primary_column', None)
        return cls(meta_instance.transform_type, meta_instance.source_column, meta_instance.destination_column, primary_column, meta_instance.split_by, meta_instance.pick_index)
```
This decouples specification and implementation. Bootstrap methods de-reference environment variables, and lookup information from different parts of the metastore, as required. This leaves the `__init__` sufficiently clean to make them usable directly from python, in addition to specifiable from yaml.

In the case of a `SplitPickTransform`, the actual Spark code lives in the `apply_transform(...)` method. 

Spark pipelines are constructed by the [transformation.TransformerPipeline](src/transformation.py) class. 
It creates a spark session and then connects it to an source such as [sparkio.KafkaSparkSource](src/sparkio.py) to get a read stream.
Transforms like `SplitPickTransform` are then iteratively applied. 
This includes the formation of a second audit stream by the [transformation.AuditingTransform](src/transformation.py) superclass.
Finally the transform can be `run()` which connects these two streams to appropriate sinks like [sparkio.DeltaRsSparkSink](src/sparkio.py).

## Raise the Services

Docker compose is used to raise the services defined in [docker-compose.yml](docker-compose.yml). 
There are three main services: `broker` provides kafka, 
`producer` runs the `patient_producer` transform defined in `metastore.yml`, 
and `consumer` raises a Jupyter notebook where the `patient_pseudonymiser` pipeline can be run. 
Results from the pipeline are written to `./results` mounted as a volume. 

## Examine the Results

The pipeline is best explored from the [nb/viewer.ipynb](nb/viewer.ipynb) notebook running on the `consumer` container.

## Caveats 

There are three main caveats:
1. Most obviously this is a demonstrator, there are no tests, few comments, and some loose-ends.
2. The in-notebook async generator of pandas dataframes, works nicely until you interrupt the kernel, when it abruptly takes down Jupyter. 
3. I couldn't persuade Java and Python to talk to DeltaLake via Spark, this was a version problem within the container I'd chosen and I didn't have time to investigate. Instead I used the DeltaRS library which worked like a charm. However, I have had to use a Spark `foreachBatch(..)` and then manually deduplicate via upsert, to provide exactly-once semantics, rather than rely on the real Spark DeltaLake sink. It works for me but I would be surprised if this were satisfactory at scale. 
