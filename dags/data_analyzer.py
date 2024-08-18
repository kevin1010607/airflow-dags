import os
from datetime import datetime
from enum import Enum
from time import perf_counter
from typing import List, Tuple
from warnings import simplefilter

import hdfs
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
from evidently.report import Report
from pyod.models.ecod import ECOD
from pyspark.sql import SparkSession
from sklearn.preprocessing import StandardScaler

# =========================================================================================
# ====================================== analysis =========================================
# =========================================================================================
# analysis the given data
# =========================================================================================

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

HDFS_URL = "hdfs:///hadoop-platform:9000"
HDFS_HOST = "hadoop-platform"
HDFS_PORT = 9000
HDFS_USER = "hdoop"
STORAGE_PATH = "/user/mlops"
NUMERICAL_COLS = [
    "efo_chargetime",
    "efo_chargevol",
    "efo_currentovershort_1",
    "efo_maxvoltage_1",
    "efo_maxvoltage_2",
    "efo_maxvoltage_3",
    "efo_meanchangerate_1",
    "efo_meanchangerate_2",
    "efo_meanchangerate_3",
    "efo_meancurrent_1",
    "efo_meancurrent_2",
    "efo_meancurrent_3",
    "efo_meanvoltage_1",
    "efo_meanvoltage_2",
    "efo_meanvoltage_3",
    "efo_minvoltage_1",
    "efo_minvoltage_2",
    "efo_minvoltage_3",
    "efo_risetime_1",
    "efo_risetime_2",
    "efo_risetime_3",
    "efo_settlingtime_1",
    "efo_settlingtime_2",
    "efo_settlingtime_3",
    "efo_voltagestd_1",
    "efo_voltagestd_2",
    "efo_voltagestd_3",
    "enhhpwr_maximpedance",
    "enhpwr_maxphaseangle",
    "enhpwr_meanchangerate",
    "enhpwr_meancurrent",
    "enhpwr_meanimpedance",
    "enhpwr_phaselocktime",
    "enhpwr_rampdowntime",
    "enhpwr_risetime",
    "enhpwr_settlingtime",
    "enh_drivein",
    "enh_speed",
    "fabseatpwr_maximpedance",
    "fabseatpwr_maxphaseangle",
    "fabseatpwr_meanchangerate",
    "fabseatpwr_meancurrent",
    "fabseatpwr_meanimpedance",
    "fabseatpwr_phaselocktime",
    "fabseatpwr_ramdowntime",
    "fabseatpwr_risetime",
    "fabseatpwr_settlingtime",
    "flat_height",
    "force_bouncingzoffset_2",
    "force_bouncingzoffset_3",
    "force_bouncingzoffset_4",
    "force_endzoffset_2",
    "force_endzoffset_3",
    "force_endzoffset_4",
    "force_errstd_2",
    "force_errstd_3",
    "force_errstd_4",
    "force_maxforce_2",
    "force_maxforce_3",
    "force_maxforce_4",
    "force_maxzoffsettime_2",
    "force_maxzoffsettime_3",
    "force_maxzoffsettime_4",
    "force_maxzoffset_2",
    "force_maxzoffset_3",
    "force_maxzoffset_4",
    "force_meanchangerate_2",
    "force_meanchangerate_3",
    "force_meanchangerate_4",
    "force_meanforce_2",
    "force_meanforce_3",
    "force_meanforce_4",
    "force_meanzoffset_2",
    "force_meanzoffset_3",
    "force_meanzoffset_4",
    "force_minforce_2",
    "force_minforce_3",
    "force_minforce_4",
    "force_minzoffsettime_2",
    "force_minzoffsettime_3",
    "force_minzoffsettime_4",
    "force_minzoffset_2",
    "force_minzoffset_3",
    "force_minzoffset_4",
    "force_risetime_2",
    "force_risetime_4",
    "force_stdzoffset_2",
    "force_stdzoffset_3",
    "force_stdzoffset_4",
    "force_settlingtime_2",
    "force_settlingtime_4",
    "force_totalendzoffset_2",
    "force_totalendzoffset_3",
    "force_totalendzoffset_4",
    "heat_bond",
    "heat_post",
    "heat_pre",
    "ref_contactvelocityerror",
    "ref_height",
    "scrub_dist_3",
    "scrub_maxamp_3",
    "scrub_minamp_3",
    "scrub_time_3",
    "search_daczmean",
    "search_height",
    "search_maxsearchdelay",
    "search_maxvelocityerror",
    "search_maxxyposerror",
    "search_samplingtime",
    "search_searchdelay",
    "search_speed",
    "search_time",
    "stbypwr_maximpedance",
    "stbypwr_maxphaseangle",
    "stbypwr_meanchangerate",
    "stbypwr_meancurrent",
    "stbypwr_meanimpedance",
    "stbypwr_phaselocktime",
    "stbypwr_risetime",
    "stbypwr_settlingtime",
    "tail_ht",
    "tail_htime",
    "touch_height",
    "touch_time",
    "usg_maximpedance_2",
    "usg_maximpedance_3",
    "usg_maximpedance_4",
    "usg_maxphaseangle_3",
    "usg_maxphaseangle_4",
    "usg_meanchangerate_2",
    "usg_meanchangerate_3",
    "usg_meanchangerate_4",
    "usg_meancurrent_2",
    "usg_meancurrent_3",
    "usg_meancurrent_4",
    "usg_meanimpedance_2",
    "usg_meanimpedance_3",
    "usg_meanimpedance_4",
    "usg_on_2",
    "usg_phaselocktime_2",
    "usg_phaselocktime_3",
    "usg_phaselocktime_4",
    "usg_rampdowntime_4",
    "usg_risetime_2",
    "usg_risetime_3",
    "usg_risetime_4",
    "usg_settlingtime_2",
    "usg_settlingtime_3",
    "usg_settlingtime_4",
    "xpower_amplitude_4",
    "xpower_frequency_4",
    "zsoftrelease_ht",
    "zsoftrelease_time",
    "bond_position_x",
    "bond_position_y",
    "wire_angle",
    "wire_length",
    "row_no",
    "create_force_endposition_2",
    "create_force_endposition_3",
    "create_force_endposition_4",
    "create_ref_touch",
    "create_force_totalzoffset_max",
    "create_efo_area",
    "create_efo_firstovershoot",
    "create_efo_totaltime_1",
    "create_efo_totaltime_2",
    "create_efo_totaltime_3",
]
REFERENCE_ROW_SIZE = 400000
REFERENCE_COLUMN_BATCH_SIZE = 40
CURRENT_COLUMN_BATCH_SIZE = 100


class TaskStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class BaseTask:
    def __init__(self, context):
        self.context = context
        self.dag_run_id = context["dag_run"].run_id

    def extract_params(self) -> Tuple[List[str], List[str]]:
        """Extracts the date and lot_id from the context

        :return Tuple[List[str], List[str]]: the date and lot_id
        """
        date = self.context["params"]["date"]
        lot_id = self.context["params"]["lot_id"]
        if isinstance(date, str):
            date = [date]
        if isinstance(lot_id, str):
            lot_id = [lot_id]
        return date, lot_id

    def get_spark_client(self, name: str) -> SparkSession:
        """Gets the configured spark client.

        :param str name: the name of the spark client

        :return SparkSession: the spark client
        """
        spark_client = (
            SparkSession.Builder()
            .appName(name)
            .master("local[1]")
            .config(
                "spark.hive.metastore.uris", "thrift://hadoop-platform:9083"
            )
            .config(
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
                2,
            )
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark_client

    def run(self):
        raise NotImplementedError("run method is not implemented")

    def write_df_to_hdfs_csv(self, df: pd.DataFrame, path: str):
        path = path.replace(":", "-")
        ahdfs = fs.HadoopFileSystem(HDFS_HOST, HDFS_PORT, user=HDFS_USER)
        adf = pa.Table.from_pandas(df)
        pc.write_csv(adf, path, filesystem=ahdfs)

    def read_csv_from_hdfs(self, path: str) -> pd.DataFrame:
        path = path.replace(":", "-")
        ahdfs = fs.HadoopFileSystem(HDFS_HOST, HDFS_PORT, user=HDFS_USER)
        df = pc.read_csv(path, filesystem=ahdfs).to_pandas()
        return df

    def write_df_to_hdfs_parquet(self, df: pd.DataFrame, path: str):
        path = path.replace(":", "-")
        ahdfs = fs.HadoopFileSystem(HDFS_HOST, HDFS_PORT, user=HDFS_USER)
        adf = pa.Table.from_pandas(df)
        pq.write_table(adf, path, filesystem=ahdfs)

    def read_parquet_from_hdfs(self, path: str) -> pd.DataFrame:
        path = path.replace(":", "-")
        ahdfs = fs.HadoopFileSystem(HDFS_HOST, HDFS_PORT, user=HDFS_USER)
        df = pq.read_table(path, filesystem=ahdfs).to_pandas()
        return df

    def write_str_to_hdfs(self, s: str, path: str):
        path = path.replace(":", "-")
        hdfs_client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)
        with hdfs_client.write(path) as writer:
            writer.write(s.encode())

    def rm_hdfs(self, path: str, recursive=False) -> bool:
        """Removes the file or directory in hdfs

        Replace the colon in the path with a dash to avoid hdfs error

        :param str path: the path to remove
        :return bool: the status of the removal, True if successful, False if the path does not exist
        """
        path = path.replace(":", "-")
        hdfs_client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)
        status = hdfs_client.delete(path, recursive=recursive)
        return status


class CollectReferenceTask(BaseTask):
    def __init__(self, context):
        super().__init__(context)

    def run(self):
        try:
            date, lot_id = self.extract_params()
            print(f"Dag run id: {self.dag_run_id}")
            print(f"Collecting data for date: {date} and lot_id: {lot_id}")

            # Collect
            start_collect_ts = perf_counter()
            spark_client = self.get_spark_client(
                "data_analyzer_collect_reference"
            )
            min_date = min(
                date, key=lambda x: datetime.strptime(x, "%Y-%m-%d")
            )
            reference_df = self.collect_data_before_date(
                spark_client, "motion_bb_mfdotwbdot000001", min_date
            )
            spark_client.stop()
            print(
                f"Collecting reference data took {perf_counter() - start_collect_ts} sec."
            )

            # Save
            start_save_ts = perf_counter()
            path = os.path.join(STORAGE_PATH, self.dag_run_id)
            reference_path = os.path.join(path, "reference_data.parquet")
            # Save to hdfs
            self.write_df_to_hdfs_parquet(reference_df, reference_path)
            print(
                f"Saving reference data took {perf_counter() - start_save_ts} sec."
            )

            return {
                "reference_path": reference_path,
                "status": TaskStatus.SUCCESS.value,
                "error": "",
            }
        except Exception as e:
            print(f"Error during collect_reference: {str(e)}")
            return {
                "reference_path": None,
                "status": TaskStatus.FAILED.value,
                "error": str(e),
            }

    def collect_data_before_date(
        self, spark_client: SparkSession, table: str, date: str
    ) -> pd.DataFrame:
        """Collects the latest data as reference data from the given table and before the date.
        Collects the data in batches to avoid memory issues.

        :param SparkSession spark_client: the spark client
        :param str table: the table name
        :param str date: the date, in format 'YYYY-MM-DD'

        :return pd.DataFrame: the collected data
        """
        columns = spark_client.sql(f"SELECT * FROM {table} LIMIT 1").columns
        print(f"columns: {columns}")
        print(f"len(columns): {len(columns)}")

        tot_df: pd.DataFrame = None
        for i in range(0, len(columns), REFERENCE_COLUMN_BATCH_SIZE):
            print(f"i: {i}")
            j = min(i + REFERENCE_COLUMN_BATCH_SIZE, len(columns))
            df = spark_client.sql(
                f"SELECT {', '.join(columns[i:j])} FROM {table} WHERE date < '{date}' ORDER BY datetime DESC LIMIT {REFERENCE_ROW_SIZE}"
            ).toPandas()

            if tot_df is None:
                tot_df = pd.DataFrame(df)
            else:
                tot_df = pd.concat([tot_df, pd.DataFrame(df)], axis=1)

        return tot_df


class CollectCurrentTask(BaseTask):
    def __init__(self, context):
        super().__init__(context)

    def run(self):
        try:
            date, lot_id = self.extract_params()
            print(f"Dag run id: {self.dag_run_id}")
            print(f"Collecting data for date: {date} and lot_id: {lot_id}")

            # Collect
            start_collect_ts = perf_counter()
            spark_client = self.get_spark_client(
                "data_analyzer_collect_current"
            )
            current_df = self.collect_data_by_pairs(
                spark_client, "motion_bb_mfdotwbdot000001", date, lot_id
            )
            spark_client.stop()
            print(
                f"Collecting current data took {perf_counter() - start_collect_ts} sec."
            )

            # Save
            start_save_ts = perf_counter()
            path = os.path.join(STORAGE_PATH, self.dag_run_id)
            current_path = os.path.join(path, "current_data.parquet")
            # Save to hdfs
            self.write_df_to_hdfs_parquet(current_df, current_path)
            print(
                f"Saving current data took {perf_counter() - start_save_ts} sec."
            )

            return {
                "current_path": current_path,
                "status": TaskStatus.SUCCESS.value,
                "error": "",
            }
        except Exception as e:
            print(f"Error during collect_current: {str(e)}")
            return {
                "current_path": None,
                "status": TaskStatus.FAILED.value,
                "error": str(e),
            }

    def collect_data_by_pair(
        self, spark_client: SparkSession, table: str, date: str, lot_id: str
    ) -> pd.DataFrame:
        """Collects the data from the given table and date-lot_id pair.
        Collects the data in batches to avoid memory issues.

        :param SparkSession spark_client: the spark client
        :param str table: the table name
        :param str date: the date, in format 'YYYY-MM-DD'
        :param str lot_id: the lot id

        :return pd.DataFrame: the collected data
        """
        columns = spark_client.sql(f"SELECT * FROM {table} LIMIT 1").columns
        print(f"columns: {columns}")
        print(f"len(columns): {len(columns)}")

        tot_df: pd.DataFrame = None
        for i in range(0, len(columns), CURRENT_COLUMN_BATCH_SIZE):
            print(f"i: {i}")
            j = min(i + CURRENT_COLUMN_BATCH_SIZE, len(columns))
            df = spark_client.sql(
                f"SELECT {', '.join(columns[i:j])} FROM {table} WHERE date = '{date}' AND lot_id = '{lot_id}'"
            ).toPandas()

            if tot_df is None:
                tot_df = pd.DataFrame(df)
            else:
                tot_df = pd.concat([tot_df, pd.DataFrame(df)], axis=1)

        return tot_df

    def collect_data_by_pairs(
        self,
        spark_client: SparkSession,
        table: str,
        date: List[str],
        lot_id: List[str],
    ) -> pd.DataFrame:
        """Collects the data from the given table and date-lot_id pairs.

        :param SparkSession spark_client: the spark client
        :param str table: the table name
        :param List[str] date: the list of dates
        :param List[str] lot_id: the list of lot ids

        :return pd.DataFrame: the collected data
        """
        tot_df: pd.DataFrame = None
        for d, l in zip(date, lot_id):
            df = self.collect_data_by_pair(spark_client, table, d, l)
            if tot_df is None:
                tot_df = pd.DataFrame(df)
            else:
                tot_df = pd.concat([tot_df, pd.DataFrame(df)], axis=0)

        return tot_df


class DetectOutliersTask(BaseTask):
    def __init__(self, context):
        super().__init__(context)

    def run(self, collect_current_task):
        try:
            if collect_current_task["status"] == TaskStatus.FAILED.value:
                return {
                    "outliers_path": None,
                    "status": TaskStatus.SKIPPED.value,
                    "error": "",
                }

            print(f"Dag run id: {self.dag_run_id}")

            # Load
            start_load_ts = perf_counter()
            current_path = collect_current_task["current_path"]
            current_df: pd.DataFrame
            current_df = self.read_parquet_from_hdfs(current_path)
            print(
                f"Loading current data took {perf_counter() - start_load_ts} sec."
            )

            # Detect
            start_detect_ts = perf_counter()
            outliers = self.outlier_detection(current_df)
            print(
                f"Detecting outliers took {perf_counter() - start_detect_ts} sec."
            )

            # Save
            start_save_ts = perf_counter()
            path = os.path.join(STORAGE_PATH, self.dag_run_id)
            outliers_path = os.path.join(path, "outliers.csv")
            self.write_df_to_hdfs_csv(outliers, outliers_path)
            print(
                f"Saving outliers took {perf_counter() - start_save_ts} sec."
            )

            return {
                "outliers_path": outliers_path,
                "status": TaskStatus.SUCCESS.value,
                "error": "",
            }
        except Exception as e:
            print(f"Error during detect_outliers: {str(e)}")
            return {
                "outliers_path": None,
                "status": TaskStatus.FAILED.value,
                "error": str(e),
            }

    def outlier_detection(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Detects the outliers in the given data

        :param pd.DataFrame raw_data: the raw data

        :return pd.DataFrame: the outlier rows
        """
        numerical_cols = [
            col for col in NUMERICAL_COLS if col in raw_data.columns
        ]

        # get the numerical data
        numerical_data = raw_data[numerical_cols]
        # standardize the data
        scaler = StandardScaler()
        numerical_data = scaler.fit_transform(numerical_data)

        # remove the outlier from the data
        # TODO(saohsuan): change the model to the correct model
        contamination = 0.001
        clf = ECOD(contamination=contamination)
        y = clf.fit_predict(numerical_data).astype(bool)
        outliers = raw_data[y]
        return outliers


class DetectDriftTask(BaseTask):
    def __init__(self, context):
        super().__init__(context)

    def run(self, collect_reference_task, collect_current_task):
        try:
            if (
                collect_reference_task["status"] == TaskStatus.FAILED.value
                or collect_current_task["status"] == TaskStatus.FAILED.value
            ):
                return {
                    "drift_report_json_path": None,
                    "drift_report_html_path": None,
                    "status": TaskStatus.SKIPPED.value,
                    "error": "",
                }

            print(f"Dag run id: {self.dag_run_id}")

            # Load
            start_load_ts = perf_counter()
            reference_path = collect_reference_task["reference_path"]
            current_path = collect_current_task["current_path"]
            reference_df = self.read_parquet_from_hdfs(reference_path)
            # if the reference data is empty, skip the drift detection
            if reference_df.empty:
                return {
                    "drift_report_json_path": None,
                    "drift_report_html_path": None,
                }
            current_df = self.read_parquet_from_hdfs(current_path)
            print(
                f"Loading reference and current data took {perf_counter() - start_load_ts} sec."
            )

            # Detect
            start_detect_ts = perf_counter()
            report_json_str, report_html_str = self.drift_detection(
                reference_df, current_df
            )
            print(
                f"Detecting drift took {perf_counter() - start_detect_ts} sec."
            )

            # Save
            start_save_ts = perf_counter()
            path = os.path.join(STORAGE_PATH, self.dag_run_id)
            drift_report_json_path = os.path.join(path, "drift_report.json")
            drift_report_html_path = os.path.join(path, "drift_report.html")
            self.write_str_to_hdfs(report_json_str, drift_report_json_path)
            self.write_str_to_hdfs(report_html_str, drift_report_html_path)
            print(
                f"Saving drift report took {perf_counter() - start_save_ts} sec."
            )

            return {
                "drift_report_json_path": drift_report_json_path,
                "drift_report_html_path": drift_report_html_path,
                "status": TaskStatus.SUCCESS.value,
                "error": "",
            }
        except Exception as e:
            return {
                "drift_report_json_path": None,
                "drift_report_html_path": None,
                "status": TaskStatus.FAILED.value,
                "error": str(e),
            }

    def drift_detection(
        self,
        reference_data: pd.DataFrame,
        current_data: pd.DataFrame,
    ) -> Tuple[str, str]:
        """Detects the drift in current data compared to the reference data

        Args:
        reference_data (pd.DataFrame): the reference data
        current_data (pd.DataFrame): the current data

        Returns:
        report_json_str (str): the json string of the drift report
        report_html_str (str): the html string of the drift report
        """
        print("Detecting drift...")
        numerical_cols = [
            col for col in NUMERICAL_COLS if col in reference_data.columns
        ]
        cols = numerical_cols + ["datetime"]
        reference_data = reference_data[cols]
        current_data = current_data[cols]
        print(
            f"Detecting drift on reference{reference_data.shape} and current{current_data.shape} data..."
        )

        reference_data["datetime"] = pd.to_datetime(reference_data["datetime"])
        current_data["datetime"] = pd.to_datetime(current_data["datetime"])

        column_mapping = ColumnMapping()
        column_mapping.numerical_features = numerical_cols
        column_mapping.datetime = "datetime"
        report = Report(
            metrics=[
                DataDriftPreset(
                    num_stattest="wasserstein",
                ),
                DataQualityPreset(),
            ],
            options={"calculate_metrics": True, "calculate_p_values": True},
        )
        report.run(
            reference_data=reference_data,
            current_data=current_data,
            column_mapping=column_mapping,
        )
        report_json_str = report.json()
        report_html_str = report.get_html()

        print(f"Drift report: {report_json_str}")
        return report_json_str, report_html_str


class CleanupTask(BaseTask):
    def __init__(self, context):
        super().__init__(context)

    def run(
        self,
        collect_reference_task,
        collect_current_task,
        detect_outliers_task,
        detect_drift_task,
    ):
        try:
            # Cleanup reference and current data
            start_cleanup_ts = perf_counter()
            if collect_reference_task["status"] == TaskStatus.SUCCESS.value:
                reference_path = collect_reference_task["reference_path"]
                self.rm_hdfs(reference_path)
            if collect_current_task["status"] == TaskStatus.SUCCESS.value:
                current_path = collect_current_task["current_path"]
                self.rm_hdfs(current_path)
            print(
                f"Cleaning up reference and current data took {perf_counter() - start_cleanup_ts} sec."
            )
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")


# Define the arguments for the DAG
dag_id = "data_analyzer"
description = (
    "collect the data from the given source and detect ouliers on the data"
)
params = {
    "date": Param(["2024-01-01"], description="the date of the data"),
    "lot_id": Param(
        ["ATWLOT-010124-0932-555-003"], description="the lot id of the data"
    ),
}


@dag(
    dag_id=dag_id,
    description=description,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    params=params,
    catchup=False,
)
def data_analyzer():
    # Tasks
    @task()
    def collect_reference():
        context = get_current_context()
        collect_reference_task = CollectReferenceTask(context)
        return collect_reference_task.run()

    @task()
    def collect_current():
        context = get_current_context()
        collect_current_task = CollectCurrentTask(context)
        return collect_current_task.run()

    @task()
    def detect_outliers(collect_current_task):
        context = get_current_context()
        detect_outliers_task = DetectOutliersTask(context)
        return detect_outliers_task.run(collect_current_task)

    @task()
    def detect_drift(collect_reference_task, collect_current_task):
        context = get_current_context()
        detect_drift_task = DetectDriftTask(context)
        return detect_drift_task.run(
            collect_reference_task, collect_current_task
        )

    @task()
    def cleanup(
        collect_reference_task,
        collect_current_task,
        detect_outliers_task,
        detect_drift_task,
    ):
        context = get_current_context()
        cleanup_task = CleanupTask(context)
        return cleanup_task.run(
            collect_reference_task,
            collect_current_task,
            detect_outliers_task,
            detect_drift_task,
        )

    # Dag
    collect_reference_task = collect_reference()
    collect_current_task = collect_current()
    detect_outliers_task = detect_outliers(collect_current_task)
    detect_drift_task = detect_drift(
        collect_reference_task, collect_current_task
    )
    cleanup(
        collect_reference_task,
        collect_current_task,
        detect_outliers_task,
        detect_drift_task,
    )


data_analyzer()
