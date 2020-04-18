class EmrStepFactory:

    @staticmethod
    def get_emr_etl_step(step_name: str,
                         script_name: str,
                         raw_bucket_name: str,
                         analytics_bucket_name: str,
                         action_on_fail: str = "CONTINUE"):
        """
        Creates an a json-object to submit to a jobflow spark emr cluster.
        Args:
            step_name: name of the step
            script_name: Script to execute. The script needs to be located under
                        `/home/hadoop/spark_etl/` on the emr cluster
            raw_bucket_name: raw bucket name without `s3://` prefix
            analytics_bucket_name: bucket name where to store the results without `s3://` prefix
            action_on_fail: emr action on failure

        Returns:
            json-like object to submit as jobflow step.
        """
        return {
            "Name": f"{step_name}",
            "ActionOnFailure": f"{action_on_fail}",
            'HadoopJarStep': {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    f"/home/hadoop/spark_etl/{script_name}",
                    "--in",
                    f"s3://{raw_bucket_name}",
                    "--out",
                    f"s3://{analytics_bucket_name}",
                    "--emr-mode"
                ]
            }
        }

    @staticmethod
    def get_emr_copy_step(step_name,
                          source_bucket_name,
                          target_path="/home/hadoop/",
                          action_on_fail: str = "CANCEL_AND_WAIT"):
        """
        Copy a given bucket to the hadoop fs of a spark/emr instance.
        Args:
            step_name: name of the step
            source_bucket_name: source bucket name without `s3://` prefix
            target_path: path on the hadoop fs
            action_on_fail: emr action on failure

        Returns:
            json-like object to submit as jobflow step.
        """
        return {
            "Name": step_name,
            "ActionOnFailure": action_on_fail,
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["aws", "s3", "cp", f"s3://{source_bucket_name}", target_path, '--recursive']
            }
        }

    @staticmethod
    def get_emr_quality_step(step_name: str,
                             source_bucket_name: str,
                             script_name: str = 'lake_quality_analysis.py',
                             prefix: str = '',
                             min_count: int = 1,
                             action_on_fail: str = "CANCEL_AND_WAIT"):
        """
        Copy a given bucket to the hadoop fs of a spark/emr instance.
        Args:
            step_name: name of the step
            source_bucket_name: source bucket name without `s3://` with files in parquet format.
            script_name: path on the hadoop fs
            prefix: bucket prefix to distinguish subdirs in the same bucket
            min_count: min. count of expected rows in the checked bucket
            action_on_fail: emr action on failure
        Returns:
            json-like object to submit as jobflow step.
        """
        return {
            "Name": f"{step_name}",
            "ActionOnFailure": f"{action_on_fail}",
            'HadoopJarStep': {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    f"/home/hadoop/spark_etl/{script_name}",
                    "--in",
                    f"s3://{source_bucket_name}/{prefix}",
                    "--min-count",
                    f"{min_count}",
                    "--emr-mode"
                ]
            }
        }
