import argparse
import bz2
import logging
import os
import pickle
import sys
from datetime import datetime, timedelta, timezone

from requests.adapters import HTTPAdapter
from requests.exceptions import JSONDecodeError
from urllib3.util.retry import Retry
from requests_cache import CachedSession


class PDBFetcher:
    """Fetcher for PeeringDB's IXP information.

    This fetcher retrieves the IXP information like name and peering LANs as well as IXP
    member information (IP -> ASN). One run creates three files:
      1. pdb.raw.YYYYMMDD.pickle.bz2: Contains unprocessed request results
      2. pdb.ix.YYYYMMDD.pickle.bz2: Contains IXP information
      3. pdb.netixlan.YYYYMMDD.pickle.bz2: Contains IXP member information
    All output files are placed within a subdirectory of the specified output directory.
    Within the output directory itself there are symlinks in the form of
    n.latest.pickle.bz2 that point towards the newest file.
    """
    API_BASE = 'https://peeringdb.com/api/'
    SUBDIR_NAME = 'data'
    OUTPUT_SUFFIX = '.pickle.bz2'
    OUTPUT_FILE_FMT = '{name}.{date}' + OUTPUT_SUFFIX
    DATE_FMT = '%Y%m%d'

    def __init__(self, output_dir: str) -> None:
        # Raw request data
        self.raw = dict()
        # Formatted IXP information
        self.ix_data_fmt = list()
        # Formatted IXP member information
        self.netixlan_data_fmt = list()

        self.output_dir = output_dir
        subdir = os.path.join(output_dir, self.SUBDIR_NAME)
        file_date = datetime.now(tz=timezone.utc).strftime(self.DATE_FMT)

        self.ix_file_name = self.OUTPUT_FILE_FMT.format(name='pdb.ix', date=file_date)
        self.ix_file = os.path.join(subdir, self.ix_file_name)
        self.ix_file_relative = os.path.join(self.SUBDIR_NAME, self.ix_file_name)

        self.netixlan_file_name = self.OUTPUT_FILE_FMT.format(name='pdb.netixlan', date=file_date)
        self.netixlan_file = os.path.join(subdir, self.netixlan_file_name)
        self.netixlan_file_relative = os.path.join(self.SUBDIR_NAME, self.netixlan_file_name)

        self.raw_file_name = self.OUTPUT_FILE_FMT.format(name='pdb.raw', date=file_date)
        self.raw_file = os.path.join(subdir, self.raw_file_name)

        self.__initialize_session()

    def __initialize_session(self) -> None:
        """Initialize a cached requests session to fetch data."""
        # PeeringDB's API has tight limits, so cache just in case something fails and we
        # have the rerun the fetcher.
        self.session = CachedSession('pdb_cache',
                                     expire_after=timedelta(days=1),
                                     stale_if_error=False)
        retry = Retry(
            backoff_factor=0.1,
            status_forcelist=(429, 500, 502, 503, 504),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def __query_pdb(self, endpoint: str, params: dict = dict()) -> dict:
        """Query the PeeringDB API with the specified endpoint and
        parameters and return the response JSON data as a dictionary.

        Always query with status = 'ok'.
        """
        url = f'{self.API_BASE}{endpoint}'
        # Always query only 'ok' entries
        params['status'] = 'ok'
        logging.info(f'Querying PeeringDB {url} with params {params}')
        try:
            r = self.session.get(url, params=params)
        except ConnectionError as e:
            logging.error(f'Failed to connect to PeeringDB: {e}')
            return dict()
        if r.status_code != 200:
            logging.error(f'PeeringDB replied with status code: {r.status_code}')
            return dict()
        try:
            json_data = r.json()
        except JSONDecodeError as e:
            logging.error(f'Failed to decode JSON reply: {e}')
            return dict()
        if 'meta' in json_data and json_data['meta']:
            logging.warning(f'There was metadata: {json_data["meta"]}')
        return json_data

    def __fetch_ix_data(self) -> bool:
        """Fetch ix/ixlan/ixpfx data from PeeringDB and create formatted values.

        Create one value per ixpfx (peering LAN). The value also contains information
        about the corresponding IXP (id, name, name_long, and country) as well as the
        ixlan_id it belongs to.

        Return True if an error occurred.
        """
        # Getting the ix_id from the ixpfx requires an additional hop over
        # the ixlan since there is no direct connection.
        # Get ix data.
        ix_data = self.__query_pdb('ix')
        if not ix_data:
            return True
        self.raw['ix'] = ix_data
        ix_data_dict = dict()
        ixlan_ix_map = dict()
        for entry in ix_data['data']:
            ix_id = entry['id']
            if ix_id in ix_data_dict:
                logging.warning(f'Duplicate ix_id: {ix_id}. Ignoring entry {entry}')
                continue
            ix_data_dict[ix_id] = entry
        # Get ixlan data.
        ixlan_data = self.__query_pdb('ixlan')
        if not ixlan_data:
            return True
        self.raw['ixlan'] = ixlan_data
        # Construct a map ixlan_id -> ix_id.
        ixlan_ix_map = dict()
        for entry in ixlan_data['data']:
            ixlan_id = entry['id']
            ix_id = entry['ix_id']
            if ixlan_id in ixlan_ix_map:
                logging.warning(f'Duplicate ixlan_id: {ixlan_id}. Already present for ix_id {ixlan_ix_map[ixlan_id]}, '
                                f'would overwrite with {ix_id}')
                continue
            ixlan_ix_map[ixlan_id] = ix_id
        # Get ixpfx data.
        ixpfx_data = self.__query_pdb('ixpfx')
        if not ixpfx_data:
            return True
        self.raw['ixpfx'] = ixpfx_data
        for entry in ixpfx_data['data']:
            ixpfx_id = entry['id']
            proto = entry['protocol']
            if proto != 'IPv4' and proto != 'IPv6':
                logging.warning(f'Unknown protocol specified for ixpfx {ixpfx_id}: {proto}')
                continue
            ixlan_id = entry['ixlan_id']
            if ixlan_id not in ixlan_ix_map:
                logging.warning(f'Failed to find ixlan {ixlan_id} for ixpfx {ixpfx_id}.')
                continue
            ix_id = ixlan_ix_map[ixlan_id]
            if ix_id not in ix_data_dict:
                logging.warning(f'Failed to find ix {ix_id} for ixlan {ixlan_id} / ixpfx {ixpfx_id}.')
                continue
            ix_info = ix_data_dict[ix_id]
            value = {'ix_id': ix_id,
                     'name': ix_info['name'],
                     'name_long': ix_info['name_long'],
                     'country': ix_info['country'],
                     'ixlan_id': ixlan_id,
                     'ixpfx_id': ixpfx_id,
                     'protocol': proto,
                     'prefix': entry['prefix']}
            self.ix_data_fmt.append(value)
        return False

    @staticmethod
    def get_netixlan_message_dict(data: dict) -> dict:
        """Copy only relevant fields, rename fields where necessary, and
        return the reduced dictionary.
        """
        ret = dict()
        ret['ix_id'] = data['ix_id']
        ret['name'] = data['name']
        ret['netixlan_id'] = data['id']
        ret['asn'] = data['asn']
        ret['ipaddr4'] = data['ipaddr4']
        ret['ipaddr6'] = data['ipaddr6']
        return ret

    def __fetch_netixlan_data(self) -> bool:
        """Fetch netixlan data from PeeringDB and create formatted values.

        Only fields that are specified in the get_message_dict method are included in
        the formatted values.

        Return True if an error occurred.
        """
        netixlan_data = self.__query_pdb('netixlan')
        if not netixlan_data:
            return True
        self.raw['netixlan'] = netixlan_data
        for entry in netixlan_data['data']:
            self.netixlan_data_fmt.append(self.get_netixlan_message_dict(entry))
        return False

    @staticmethod
    def make_symlink(src: str, dst: str) -> bool:
        """Create a symlink from src to dst.

        To update existing symlinks, they need to be deleted first. Raise an error if
        dst already exists but is not a symlink.

        Also symlink syntax is weird, since the actual link will be
          dst -> src
        i.e., src is the "real" file, and the link actually points towards it.

        Return True if an error occurred.
        """
        if os.path.islink(dst):
            os.remove(dst)
        elif os.path.exists(dst):
            logging.error(f'Can not update symlink {dst} -> {src}')
            logging.error('Destination exists, but is not a symlink.')
            return True
        os.symlink(src, dst)
        return False

    def __update_symlinks(self) -> bool:
        """Update the symlinks of the output files.

        Return True if an error occurred.
        """
        error = False
        ix_symlink = os.path.join(self.output_dir, self.OUTPUT_FILE_FMT.format(name='pdb.ix', date='latest'))
        if self.make_symlink(self.ix_file_relative, ix_symlink):
            error = True
        netixlan_symlink = os.path.join(self.output_dir,
                                        self.OUTPUT_FILE_FMT.format(name='pdb.netixlan', date='latest'))
        if self.make_symlink(self.netixlan_file_relative, netixlan_symlink):
            error = True
        return error

    @staticmethod
    def dump_to_pickle_bz2(data, file):
        """Dump data to file in bzip2ed pickle format."""
        logging.info(f'Writing {file}')
        with bz2.open(file, 'wb') as f:
            pickle.dump(data, f)

    def __dump(self) -> None:
        """Create output directory and dump all files."""
        os.makedirs(os.path.dirname(self.ix_file), exist_ok=True)
        self.dump_to_pickle_bz2(self.ix_data_fmt, self.ix_file)
        self.dump_to_pickle_bz2(self.netixlan_data_fmt, self.netixlan_file)
        self.dump_to_pickle_bz2(self.raw, self.raw_file)

    def run(self) -> bool:
        """Run the fetcher.

        Fetch IXP information, IXP member information, write the data to file and update
        the symlinks. If errors occur during execution, it will not be stopped, but
        reported in the end.

        Return True if an error occurred.
        """
        error = False
        if self.__fetch_ix_data():
            error = True
        if self.__fetch_netixlan_data():
            error = True
        self.__dump()
        if self.__update_symlinks():
            error = True
        return error


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output-dir', help='overwrite default output directory', default='pdb_dumps')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.FileHandler('fetch-pdb-dump.log'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    fetcher = PDBFetcher(args.output_dir)
    if fetcher.run():
        logging.error('Something went wrong.')
        sys.exit(1)


if __name__ == '__main__':
    main()
    sys.exit(0)
