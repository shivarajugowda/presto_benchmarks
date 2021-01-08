# __main__.py
import argparse
from .presto_benchmarks import PrestoBenchmarks

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-f", "--config_file", default="config.yaml", help="Configuration file to load.")
    parser.add_argument("-d", "--description", default="", help="Short Benchmark Description")
    ARGS = parser.parse_args()
    PrestoBenchmarks.run(ARGS.config_file, ARGS.description)