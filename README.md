# A Spark Streaming Pipeline Defined in YAML

This repo contains the result of a 4-day data-engineering hack to process records from Kafka using a Spark Structured Streaming pipeline, defined by a YAML specification, with docker compose to raise a small collection of containers. 
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
Diagnoses are taken from NHS Digital Primary Care SNOMED code aggregate usage.
Their frequency in this sample data match their frequencies in the source dataset. 

It is always comforting when synthetic data look distinctly unreal. 

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
A number of keys are defined in environment variables, especially where values are the remit of devops 
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
The metastore defines three pipelines. The first, `patient_producer`, is a small service which reads the `sample_data.json` fixture and outputs patients
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
The second pipeline, `patient_pseudonymiser`, connects to the kafka source, streams outputs to a deltalake table, and sends audit records to a file. 
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
The last removes the final section of the postcode, e.g. from `BR5 2RE` to `BR5`. 

A third pipeline, `memory_patient_pseudonymiser`, presents another version of the one above, but whose sinks are all in-memory. 
They are wrapped in an asynchronous Python generator, which yields dataframes that can be `display()`ed continuously within a notebook. 

## Pipeline Construction

The `metastore.yaml` file is validated using `Pydantic` models defined in [src/meta.py](src/meta.py).
Each model has an `infraclass` property, which defines the corresponding pipeline implementation code. For example, a meta-model instance `meta_instance` of class `meta.Translate`:
```python
class Translate(Transform):
    transform_type: Literal['translate']
    fixture_filepath_env: str
    key_column: str
    value_column: str
    infraclass: Type[transformation.TranslateTransform] = transformation.TranslateTransform
```
is implemented by calling `infra_instance = meta_instance.infraclass.bootstrap(meta_instance...)`, defined in the class `transformation.TranslateTransform`:
```python
class TranslateTransform(AuditingTransform, Infraclass):
    def __init__(self, transform_name, source_column, destination_column, primary_column, fixtures_path, key_column, value_column):
        tag_params = dict(fixture=Path(fixtures_path).stem, key_column=key_column, value_column=value_column)
        super().__init__(transform_name, source_column, destination_column, primary_column, tag_params)
        self.fixtures_path, self.key_column, self.value_column = fixtures_path, key_column, value_column

    def apply_transform(self, stream_df, spark_session):
        translation_df = spark_session.read.option('inferSchema', 'true').option('header','true').parquet(self.fixtures_path)
        translation_df = translation_df.withColumnRenamed(self.key_column, self.source_column).withColumnRenamed(self.value_column, self.destination_column)
        translation_df.cache()
        transformed_stream = stream_df.join(translation_df, on=self.source_column, how='left')
        return transformed_stream

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        primary_column = kwargs.get('primary_column', None)
        fixture_filepath = os.environ.get(meta_instance.fixture_filepath_env, default_env.get(meta_instance.fixture_filepath_env))
        return cls(meta_instance.transform_type, meta_instance.source_column, meta_instance.destination_column, primary_column, fixture_filepath, meta_instance.key_column, meta_instance.value_column)
```
It decouples specification and implementation. Bootstrap methods de-reference environment variables, and lookup information from different parts of the metastore, as required. This leaves `TranslateTransform.__init__` sufficiently clean to make it usable directly from Python. Python allows only one initializer - secondary instantiation from a class method is commonplace. 

In the case of a `TranslateTransform`, the actual Spark code lives in the `apply_transform(...)` method. 
This transform works by loading in a static translation table, and joining it on `self.source_column` to the incoming `stream_df`.

Spark pipelines are constructed by the [transformation.TransformerPipeline](src/transformation.py) class. 
It creates a spark session and then connects it to a source such as [sparkio.KafkaSparkSource](src/sparkio.py) to get a read stream.
Transforms like `SplitPickTransform` are then iteratively applied. 
This includes the formation of a second audit stream by the [transformation.AuditingTransform](src/transformation.py) superclass.
Finally the transform can be `run()` which connects these two streams to appropriate sinks like [sparkio.DeltaRsSparkSink](src/sparkio.py).

Audit stream formation is somewhat interesting. Scattering use of the pyspark API over a class hierarchy, compared to issuing pyspark calls
sequentially in a simple script, add some complexity for the developer. Conversely, `transformation.AuditingTransform` which is the parent of the `TranslateTransform` shown above, shows how encapsulation can reduce error-prone code repetition and thus provides a developer benefit. 

```python
class AuditingTransform(Transform):
    def __init__(self, transform_name, source_column, destination_column, primary_column, params):
        self.primary_column = primary_column
        self.source_column = source_column
        self.destination_column = destination_column
        self.params = params
        self.transform_name = transform_name

    def getTransformTag(self, pipeline_id, transform_index):
        quoted_params = { key: (f"'{value}'" if isinstance(value, str) else value) for key, value in self.params.items()}
        param_str = ", "+ ", ".join([f"{key}={value}" for key, value in quoted_params.items()]) if self.params else ""
        return f'{self.destination_column}#{transform_index+1} = {pipeline_id}.{self.transform_name}({self.source_column}#{transform_index}{param_str})'

    def apply(self, before_stream_df, spark_session, pipeline_id, transform_index):
        # apply the transform
        after_stream_df = self.apply_transform(before_stream_df, spark_session)
        # now create the audit record
        transform_tag = self.getTransformTag(pipeline_id, transform_index)
        before_cols, after_cols = before_stream_df.columns, after_stream_df.columns
        # consider error conditions for this transform
        if self.source_column and self.source_column not in before_cols:
            raise Exception(f'the given source column {self.source_column} for transform {transform_tag} was not found in the source datastream {before_cols}')
        elif self.destination_column not in after_cols:
            raise Exception(f'the given destination column {self.destination_column} for transform {transform_tag} must be present in the destination stream {after_cols}')
        # the new_value for audit is the same for all transforms: new_value = after_stream.destination_column
        new_value_df = after_stream_df.withColumnRenamed(self.destination_column, 'new_value')[[self.primary_column, 'new_value']]
        # at this stage we reach a point of interpretation. if the audit record had contained 'source_value' rather
        # than 'old_value' then I would have done: 'source_value = before_stream.source_column' ie the value used by the 
        # transform to produce the destination column. however, 'old_value' implies the value that was overwritten by the 
        # transform. this latter interpretation is more like a Change Data Capture record. under this latter interpretation, 
        # 'old_value' will be NULL in cases where a new column is created, and the value of before_stream.destination_column otherwise
        if self.destination_column not in before_cols:
            # then the column is new one, we have not changed any original data, so we need a null literal for old_value
            old_value_df = before_stream_df.withColumn('old_value', SQL.lit(None))[[self.primary_column, 'old_value']]
        else:
            # the destination column is changing an existing column, so old_value = before_stream.destination_column 
            old_value_df = before_stream_df.withColumnRenamed(self.destination_column, 'old_value')[[self.primary_column, 'old_value']]
        # now join the old and new value columns to form the audit records
        audit_stream_df = old_value_df.join(new_value_df, on=self.primary_column, how='inner').withColumn('transform_tag', SQL.lit(transform_tag))
        return after_stream_df, audit_stream_df

    @abstractmethod
    def apply_transform(self, stream_df, spark_session):
        return stream_df
```
Finally, part of the audit record is the `transform_tag` field, formed by `getTransformTag(...)`. It produces strings such as:
```text
region#1 = patient_pseudonymiser.translate(city#0, fixture='city_to_region', key_column='city', value_column='region')
```
which captures the id of the pipeline, the transform's name, the parameters it was given. Field indexes reflect a transform's position in its pipeline's transform list. 

## Raise the Services

Docker compose is used to raise the services defined in [docker-compose.yml](docker-compose.yml). 
There are three main services: `broker` provides kafka, 
`producer` runs the `patient_producer` transform defined in `metastore.yml`, 
and `consumer` raises a Jupyter notebook where the `patient_pseudonymiser` pipeline can be run. 
Results from the pipeline are written to `./results` mounted as a volume. 

## Examine the Results

The pipeline is best explored from the [nb/viewer.ipynb](nb/viewer.ipynb) notebook running on the `consumer` container. This shows the various inputs and outputs formatted as dataframes. 

## Caveats 

There are three main caveats:
1. Most obviously this is a demonstrator, there are no tests, few comments, and some loose-ends.
2. The in-notebook async generator of pandas dataframes, works nicely until you interrupt the kernel, when it abruptly takes down Jupyter. 
3. I couldn't persuade Java and Python to talk to DeltaLake via Spark, this was a version problem within the container I'd chosen and I didn't have time to investigate. Instead I used the DeltaRS library which worked like a charm. However, I have had to use a Spark `foreachBatch(..)` and then manually deduplicate via upsert, to provide exactly-once semantics, rather than rely on the real Spark DeltaLake sink. It works for me but I would be surprised if this were satisfactory at scale. 
