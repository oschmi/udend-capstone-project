import os

import click
import pandas as pd
from click import Path
import logging
import pathlib
from os.path import abspath
from os.path import join

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("utils.accidents")

default_path = pathlib.Path(__file__).parent.absolute()


@click.command()
@click.option("--in", "in_",
              default=abspath(join(default_path, "../../data/US_Accidents_Dec19.csv")),
              help="Input file of us-accidents as *.csv",
              type=click.Path(exists=True))
@click.option("--chunksize",
              default=10000,
              help="Number of accidents per output file")
@click.option("--out",
              default=abspath(join(default_path, "../../data/accidents")),
              help="output dir to store split accident files",
              type=click.Path())
def split(in_: Path, chunksize: int, out: Path):
    i = 0
    os.makedirs(out, exist_ok=True)
    logger.info(f"Splitting {in_} into files with {chunksize} accidents")
    for chunk_accidents in pd.read_csv(in_, chunksize=chunksize):
        outpath = os.path.join(out, f"accidents_{chunksize * i}_to_{chunksize * (i + 1)}.csv")
        logger.info(f"Writing file: {outpath}")
        chunk_accidents.to_csv(outpath, index=False)
        i += 1
    logger.info("Finished splitting!")


if __name__ == '__main__':
    split()
