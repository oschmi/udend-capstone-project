import os
import pathlib

import toml


def load_local_config(path=None, with_aws=False):
    if path is None:
        path = os.path.join(pathlib.Path(__file__).parent.absolute(), "../../pyproject.toml")
    config = toml.load(path)

    if with_aws:
        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        in_ = 's3a://' + config['S3']['RAW_DATALAKE_BUCKET'] + '/'
        out = 's3a://' + config['S3']['ANALYTICS_DATALAKE_BUCKET'] + '/'
        return in_, out

    else:
        return config["LOCAL"]["INPUT_DATA_DIR"], config["LOCAL"]["OUTPUT_DATA_DIR"]
