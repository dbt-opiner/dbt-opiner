from .BQ001_bigquery_targets_must_have_maximum_bytes_billed import BQ001
from .BQ002_bigquery_tables_should_have_clustering import BQ002
from .BQ003_bigquery_views_must_have_partition_and_cluster_description import BQ003
from .BQ004_bigquery_models_must_persist_docs import BQ004
from .O001_model_must_have_description import O001
from .O002_model_description_must_have_keywords import O002
from .O003_all_columns_must_have_description import O003
from .O004_final_columns_in_model_must_be_explicitly_named_at_least_once import O004
from .O005_model_should_have_unique_key import O005
from .P001_pii_columns_should_have_tags import P001
from .P002_project_must_not_send_anon_stats import P002


opinion_classes = [BQ001, BQ002, BQ003, BQ004, O001, O002, O003, O004, O005, P001, P002]
