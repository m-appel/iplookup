import bz2
import configparser
import logging
import os
import pickle
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from socket import AF_INET, AF_INET6

import radix

from kafka_wrapper.kafka_reader import KafkaReader


@dataclass
class Prefixes:
    prefix_count: int = 0
    prefix_ip_sum: int = 0


@dataclass
class Visibility:
    ip2asn_visible: bool = False
    ip2ixp_visible: bool = False
    ip2asn_prefixes: Prefixes = field(default_factory=Prefixes)
    ip2ixp_prefixes: Prefixes = field(default_factory=Prefixes)


class IPLookup:
    """Lookup tool that combines the knowledge of ip2asn and ip2ixp.

    Provide the same interface to look up ASN info etc. from IPs, but try both data
    sources for retrieval. Use a provided offline version of ip2asn and construct the
    data for ip2ixp from provided files or Kafka topics.

    This tool can also be used to find the most-specific prefix for an IP or to get
    information about the IP's visibility in ip2asn and/or ip2ixp.
    """

    LG_DUMP_FILE_SUFFIX = '.pickle.bz2'

    def __init__(self,
                 config: configparser.ConfigParser,
                 ip2ixp_read_ts: int = None):
        self.initialized = False
        try:
            ip2asn_dir = config.get('ip2asn', 'path').rstrip('/')
            ip2asn_db = config.get('ip2asn', 'db', fallback=f'{ip2asn_dir}/db/latest.pickle')
            ip2ixp_ix_kafka_topic = config.get('ip2ixp', 'ix_kafka_topic', fallback=None)
            ip2ixp_netixlan_kafka_topic = config.get('ip2ixp', 'netixlan_kafka_topic', fallback=None)
            ip2ixp_bootstrap_servers = config.get('ip2ixp', 'kafka_bootstrap_servers', fallback='localhost:9092')
            ip2ixp_ix_file = config.get('ip2ixp', 'ix_file', fallback=None)
            ip2ixp_netixlan_file = config.get('ip2ixp', 'netixlan_file', fallback=None)
        except configparser.NoSectionError as e:
            logging.error(f'Missing section in configuration file: {e}')
            return
        except configparser.NoOptionError as e:
            logging.error(f'Missing option in configuration file: {e}')
            return
        if (not (ip2ixp_ix_file or ip2ixp_ix_kafka_topic)
                or not (ip2ixp_netixlan_file or ip2ixp_netixlan_kafka_topic)):
            logging.error('Either file or kafka topic is required for ix and netixlan information.')
            return
        if (ip2ixp_ix_file and ip2ixp_ix_kafka_topic
                or ip2ixp_netixlan_file and ip2ixp_netixlan_kafka_topic):
            logging.error('Both file and kafka topic specified for ix or netixlan. Please decide.')
            return

        # ip2asn initialization.
        sys.path.append(ip2asn_dir)
        from ip2asn import ip2asn
        self.i2asn = ip2asn(ip2asn_db)
        self.i2asn_ipv4_asns = defaultdict(Prefixes)
        self.i2asn_ipv6_asns = defaultdict(Prefixes)
        self.__build_asn_sets_from_radix(ip2asn_db)

        # ip2ixp initialization.
        self.ixp_rtree = radix.Radix()
        if ip2ixp_ix_file:
            self.__build_ixp_rtree_from_file(ip2ixp_ix_file)
        else:
            self.__build_ixp_rtree_from_kafka(ip2ixp_ix_kafka_topic, ip2ixp_bootstrap_servers, ip2ixp_read_ts)
        self.ixp_asn_dict = dict()
        self.ixp_ipv4_asns = defaultdict(Prefixes)
        self.ixp_ipv6_asns = defaultdict(Prefixes)
        if ip2ixp_netixlan_file:
            self.__fill_ixp_asn_dict_from_file(ip2ixp_netixlan_file)
        else:
            self.__fill_ixp_asn_dict_from_kafka(ip2ixp_netixlan_kafka_topic, ip2ixp_bootstrap_servers, ip2ixp_read_ts)
        if config.has_option('ip2ixp', 'lg_dump_path'):
            self.__fill_ixp_asn_dict_from_lg_dumps(config.get('ip2ixp', 'lg_dump_path'))

        self.initialized = True

    @staticmethod
    def pickle_bz2_file_generator(file: str):
        """Provide file contents as a generator.

        The file should be in bzip2ed pickle format and contain an iterable.
        """
        with bz2.open(file, 'rb') as f:
            data = pickle.load(f)
        for e in data:
            yield e

    def __build_asn_sets_from_radix(self, db: str):
        """Record which ASes are contained in the ip2asn database as well as the number
        of prefixes and effective IP count."""
        logging.info(f'Reading ip2asn db from: {db}')
        with bz2.open(db, 'rb') as f:
            rtree: radix.Radix = pickle.load(f)
        for node in rtree:
            if 'as' not in node.data:
                logging.warning(f'Missing "as" attribute for radix node: {node}')
                continue
            if node.family == AF_INET:
                address_count = 2 ** (32 - node.prefixlen)
                self.i2asn_ipv4_asns[node.data['as']].prefix_count += 1
                self.i2asn_ipv4_asns[node.data['as']].prefix_ip_sum += address_count
            elif node.family == AF_INET6:
                address_count = 2 ** (64 - node.prefixlen)
                self.i2asn_ipv6_asns[node.data['as']].prefix_count += 1
                self.i2asn_ipv6_asns[node.data['as']].prefix_ip_sum += address_count
            else:
                logging.warning(f'Unknown protocol family {node.family} for node {node}')

    def __build_ixp_rtree(self, generator):
        """Build an rtree from peering LANs to enable ip to IXP lookup."""
        ix_id_set = set()
        prefix_set = set()
        for val in generator:
            if ('prefix' not in val
                    or 'name' not in val
                    or 'ix_id' not in val):
                logging.warning(f'Read invalid entry: {val}')
                continue
            node = self.ixp_rtree.add(val['prefix'])
            node.data['name'] = val['name']
            node.data['id'] = val['ix_id']
            ix_id_set.add(val['ix_id'])
            prefix_set.add(val['prefix'])
        logging.info(f'Loaded {len(ix_id_set)} IXPs with {len(prefix_set)} prefixes')

    def __build_ixp_rtree_from_file(self, file: str):
        """Wrapper function of __build_ixp_rtree for file source."""
        logging.info(f'Reading ix entries from file: {file}')
        self.__build_ixp_rtree(self.pickle_bz2_file_generator(file))

    def __build_ixp_rtree_from_kafka(self, topic: str, bootstrap_servers: str, start_ts: int):
        """Wrapper function of __build_ixp_rtree for Kafka source."""
        logging.info(f'Reading ix entries from Kafka topic: {topic}')
        if start_ts:
            reader = KafkaReader([topic], bootstrap_servers, start_ts)
        else:
            reader = KafkaReader([topic], bootstrap_servers)
        with reader:
            self.__build_ixp_rtree(reader.read())

    def __fill_ixp_asn_dict(self, generator):
        """Build a dictionary for IP-to-ASN mapping based in IXP membership information."""
        for val in generator:
            if ('ipaddr4' not in val
                or 'ipaddr6' not in val
                    or 'asn' not in val):
                logging.warning(f'Read invalid entry: {val}')
                continue
            if val['asn'] is None:
                continue
            asn = val['asn']
            if val['ipaddr4'] is not None:
                if val['ipaddr4'] not in self.ixp_asn_dict:
                    self.ixp_ipv4_asns[asn].prefix_count += 1
                    self.ixp_ipv4_asns[asn].prefix_ip_sum += 1
                elif self.ixp_asn_dict[val['ipaddr4']] != asn:
                    curr_as = self.ixp_asn_dict[val['ipaddr4']]
                    logging.debug(f'Updating AS entry for IP {val["ipaddr4"]}: {curr_as} -> {asn}')
                    self.ixp_ipv4_asns[curr_as].prefix_count -= 1
                    self.ixp_ipv4_asns[curr_as].prefix_ip_sum -= 1
                self.ixp_asn_dict[val['ipaddr4']] = asn
            if val['ipaddr6'] is not None:
                if val['ipaddr6'] not in self.ixp_asn_dict:
                    self.ixp_ipv6_asns[asn].prefix_count += 1
                    self.ixp_ipv6_asns[asn].prefix_ip_sum += 1
                elif self.ixp_asn_dict[val['ipaddr6']] != asn:
                    curr_as = self.ixp_asn_dict[val['ipaddr6']]
                    logging.debug(f'Updating AS entry for IP {val["ipaddr6"]}: {curr_as} -> {val["asn"]}')
                    self.ixp_ipv6_asns[curr_as].prefix_count -= 1
                    self.ixp_ipv6_asns[curr_as].prefix_ip_sum -= 1
                self.ixp_asn_dict[val['ipaddr6']] = asn
        logging.info(f'Loaded {len(self.ixp_asn_dict)} IXP IP -> AS mappings')

    def __fill_ixp_asn_dict_from_file(self, file: str):
        """Wrapper function of __fill_ixp_asn_dict for file source."""
        logging.info(f'Reading netixlan entries from file: {file}')
        self.__fill_ixp_asn_dict(self.pickle_bz2_file_generator(file))

    def __fill_ixp_asn_dict_from_kafka(self, topic: str, bootstrap_servers: str, start_ts: int):
        """Wrapper function of __fill_ixp_asn_dict for Kafka source."""
        logging.info(f'Reading netixlan entries from Kafka topic: {topic}')
        if start_ts:
            reader = KafkaReader([topic], bootstrap_servers, start_ts)
        else:
            reader = KafkaReader([topic], bootstrap_servers)
        with reader:
            self.__fill_ixp_asn_dict(reader.read())

    def __fill_ixp_asn_dict_from_lg_dumps(self, lg_dump_path: str) -> None:
        """Enhance the IP-to-ASN mapping with looking glass information.

        Looking glass information takes precedence over PeeringDB, i.e., if an IP is
        already contained in the mapping, it is overwritten.
        """
        logging.info(f'Loading route server looking glass dumps from: {lg_dump_path}')
        for entry in os.scandir(lg_dump_path):
            if not entry.is_file() or not entry.name.endswith(self.LG_DUMP_FILE_SUFFIX):
                continue
            logging.info(f'Loading dump: {entry.name}')
            try:
                with bz2.open(os.path.join(lg_dump_path, entry.name), 'rb') as f:
                    dump_data = pickle.load(f)
            except Exception as e:
                logging.error(f'Failed to load dump: {e}')
                continue
            for ip, asn in dump_data.items():
                if ip in self.ixp_asn_dict and self.ixp_asn_dict[ip] != asn:
                    logging.warning(f'Overwriting IXP member for IP {ip} from PeeringDB with looking glass data. '
                                    f'PDB: {self.ixp_asn_dict[ip]} LG: {asn}')
                self.ixp_asn_dict[ip] = asn
            logging.info(f'Loaded {len(dump_data)} IXP IP -> AS mappings')

    def ip2asn(self, ip: str) -> str:
        """Find the ASN corresponding to the given IP address."""
        asn = self.i2asn.ip2asn(ip)
        if asn != 0:
            return str(asn)
        if ip in self.ixp_asn_dict:
            return str(self.ixp_asn_dict[ip])
        return '0'

    def ip2ixpname(self, ip: str) -> str:
        """Find the IXP name corresponding to the given IP address."""
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug(f'Wrong IP address format: {ip} {e}')
            return str()
        if node is None:
            return str()
        return node.data['name']

    def ip2ixpid(self, ip: str) -> int:
        """Find the IXP id corresponding to the given IP address.

        The IXP id corresponds to PeeringDB's ix_id."""
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug(f'Wrong IP address format: {ip} {e}')
            return 0
        if node is None:
            return 0
        return node.data['id']

    def ip2prefix(self, ip: str) -> str:
        """Find the IP prefix containing the given IP address."""
        prefix = self.i2asn.ip2prefix(ip)
        if prefix:
            return prefix
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug(f'Wrong IP address format: {ip} {e}')
            return str()
        if node is None:
            return str()
        return node.prefix

    def asn2source(self, asn: str, ip_version: int = AF_INET) -> Visibility:
        """Find the source (RIB or IXP DB) as well as number of prefixes
        and IPs for the given ASN.
        """
        ret = Visibility()
        if ip_version == AF_INET:
            if asn in self.i2asn_ipv4_asns:
                ret.ip2asn_visible = True
                ret.ip2asn_prefixes = self.i2asn_ipv4_asns[asn]
            if asn in self.ixp_ipv4_asns:
                ret.ip2ixp_visible = True
                ret.ip2ixp_prefixes = self.ixp_ipv4_asns[asn]
        elif ip_version == AF_INET6:
            if asn in self.i2asn_ipv6_asns:
                ret.ip2asn_visible = True
                ret.ip2asn_prefixes = self.i2asn_ipv6_asns[asn]
            if asn in self.ixp_ipv6_asns:
                ret.ip2ixp_visible = True
                ret.ip2ixp_prefixes = self.ixp_ipv6_asns[asn]
        else:
            logging.error(f'Invalid ip_version specified: {ip_version}')
            return Visibility()
        return ret
