# `iplookup`: IP to ASN/IXP mapping

This tool combines the functionality of
[ip2asn](https://github.com/romain-fontugne/ip2asn) (IP-to-ASN mapping based on
RIBs collected by Route Views) with the knowledge of
[PeeringDB](https://www.peeringdb.com/) to provide additional information about
IXP nodes.

It uses the `ix` and `netixlan` API endpoints for information about IXP peering
LAN prefixes as well as IPs of individual IXP participants. This information
enables mapping to IXPs (using peering LAN prefixes) and additional ASN mappings
(using participant IPs).

The PeeringDB information needs to be in two Kafka topics as created by [these
producers](https://github.com/InternetHealthReport/kafka-toolbox/tree/master/peeringdb/producers).

## Installation

Clone the repository and initialize the submodule:
```
git clone --recurse-submodules git@github.com:m-appel/iplookup.git
```

Install required Python dependencies:
```
pip install -r requirements.txt -r kafka_wrapper/requirements.txt
```

## Usage

The constructor takes two parameters:

1. A `configparser.ConfigParser` instance containing required configuration options.
1. An optional timestamp from which to start reading the Kafka topics.

This repository does not include
[ip2asn](https://github.com/romain-fontugne/ip2asn) as a submodule because it
has grown quite large. Instead, you have to specify the path to an existing
installation instead. In addition, the names of the `ix` and `netixlan` Kafka
topics need to be specified.

### Example configuration

```ini
[ip2asn]
path = path/to/ip2asn
# Optional, defaults to path/db/latest.pickle
db = path/to/rib.pickle.bz2

[ip2ixp]
ix_kafka_topic = ix_topic
netixlan_kafka_topic = netixlan_topic
# Optional, defaults to localhost:9092
kafka_bootstrap_servers = kafka:9092
```

### Example code
```python
import configparser
from iplookup import IPLookup

config = configparser.ConfigParser()
config.read(configuration_file)

iplookup = IPLookup(config)
# Check if class initialized correctly.
if not iplookup.initialized:
    return

asn = iplookup.ip2asn('x.x.x.x')  # IP as a string.

```

You can/should use the `ip2ixp_read_ts` parameter (specified as UNIX Epoch **in
milliseconds**) to limit the amount of data consumed from Kafka, and to reduce
the likelihood of including stale PeeringDB entries.