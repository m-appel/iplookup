import bz2
import logging
import os
import pickle
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Iterable, Tuple

from requests.adapters import HTTPAdapter, Response
from requests.exceptions import ChunkedEncodingError
from urllib3.util.retry import Retry
from requests_futures.sessions import FuturesSession


class Crawler:
    OUTPUT_SUFFIX = '.pickle.bz2'
    DATE_FMT = '%Y%m%d'
    OUTPUT_FILE_FMT = '{name}.{date}' + OUTPUT_SUFFIX

    def __init__(self, name: str, api_url: str, output_dir: str, workers: int = 4) -> None:
        api_url = api_url.rstrip('/')
        self.urls = {
            'routeservers': f'{api_url}/routeservers',
            'neighbors': api_url + '/routeservers/{rs}/neighbors'
        }
        if not output_dir.endswith('/'):
            output_dir += '/'
        output_file_date = datetime.now(tz=timezone.utc).strftime(self.DATE_FMT)
        self.output_file = f'{output_dir}{self.OUTPUT_FILE_FMT.format(name=name, date=output_file_date)}'

        self.workers = workers
        logging.info(f'Running with {workers} workers.')
        self.data = dict()
        self.__initialize_session()

    def __initialize_session(self) -> None:
        self.session = FuturesSession(max_workers=self.workers)
        retry = Retry(
            backoff_factor=0.1,
            status_forcelist=(429, 500, 502, 503, 504),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    @staticmethod
    def decode_json(resp: Response, *args, **kwargs) -> None:
        """Process json in background"""
        logging.debug(f'Processing response: {resp.url} Status: {resp.ok}')

        try:
            resp.data = resp.json()
        except JSONDecodeError as e:
            logging.error(f'Error while reading json data: {e}')
            logging.error(resp.status_code)
            logging.error(resp.headers)
            logging.error(resp.text)
            resp.data = {}

    def fetch_urls(self, urls: list) -> Iterable:
        queries = list()
        for url in urls:
            queries.append(self.session.get(url,
                                            hooks={'response': self.decode_json},
                                            timeout=60))
        for query in queries:
            try:
                resp = query.result()
                yield resp.ok, resp.data
            except ChunkedEncodingError as e:
                logging.error(f'Failed to retrieve data for {query}')
                logging.error(e)
                return False, dict()

    def fetch_url(self, url: str) -> Tuple[bool, dict]:
        """Helper function for single URL."""
        for status, resp in self.fetch_urls([url]):
            return status, resp
        return False, dict()

    def __dump(self) -> None:
        logging.info(f'Writing output to {self.output_file}')
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with bz2.open(self.output_file, 'wb') as f:
            pickle.dump(self.data, f)

    def run(self) -> bool:
        logging.info(f'Fetching route servers from {self.urls["routeservers"]}')
        is_ok, routeservers = self.fetch_url(self.urls['routeservers'])
        if not is_ok:
            return True
        routeserver_list = routeservers['routeservers']

        logging.info(f'Fetching neighbor information from {len(routeserver_list)} route servers.')
        neighbor_urls = [self.urls['neighbors'].format(rs=rs['id']) for rs in routeserver_list]
        for is_ok, neighbor_list_root in self.fetch_urls(neighbor_urls):
            if not is_ok:
                continue
            if 'neighbors' in neighbor_list_root:
                neighbor_list = neighbor_list_root['neighbors']
            elif 'neighbours' in neighbor_list_root:
                neighbor_list = neighbor_list_root['neighbours']
            else:
                logging.error(f'Missing "neighbors"/"neighbours" field in reply: {neighbor_list_root}')
                continue
            for neighbor in neighbor_list:
                address = neighbor['address']
                asn = neighbor['asn']
                if address in self.data and self.data[address] != asn:
                    logging.warning(f'Neighbor ASN for IP {address} differs between route servers: '
                                    f'{asn} != {self.data[address]}')
                self.data[address] = asn
        logging.info(f'Got data for {len(self.data)} interfaces.')
        self.__dump()
        return False
