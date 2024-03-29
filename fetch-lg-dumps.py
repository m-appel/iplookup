import argparse
import json
import logging
import sys

from alice_lg import Crawler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--dump-raw', action='store_true', help='dump additional raw data')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='fetch-lg-dumps.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    config_file = args.config
    with open(config_file, 'r') as f:
        config = json.load(f)

    output_dir = config['output_dir']
    dump_raw = args.dump_raw
    failed_crawlers = list()
    for name, props in config['looking_glasses'].items():
        url = props['url']
        if 'workers' in props:
            crawler = Crawler(name, url, output_dir, workers=props['workers'], dump_raw=dump_raw)
        else:
            crawler = Crawler(name, url, output_dir, dump_raw=dump_raw)
        if crawler.run():
            failed_crawlers.append(name)

    if failed_crawlers:
        logging.error(f'Crawlers failed: {failed_crawlers}')


if __name__ == '__main__':
    main()
    sys.exit(0)
