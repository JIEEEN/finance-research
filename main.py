import ray
import argparse

from run.init import run as run_init
from run.debug import run as run_debug
from run.db import run as run_db
from run.update import run as run_update

parser = argparse.ArgumentParser()

parser.add_argument('-m', '--mode', choices=['init', 'debug', 'db'], default='debug', help='init_mode or update_mode')
parser.add_argument('-b', '--batch_size', default='10', help='set batch_size for get data parallel')
args = parser.parse_args()

ray.init(object_store_memory=10**9)

if __name__ == "__main__":
    if args.mode == 'init':
        run_init(args.batch_size)
    elif args.mode == 'debug':
        run_debug()
    elif args.mode == 'db':
        run_db()
    elif args.mode == 'update':
        run_update()