# `iplookup`: IP to ASN/IXP mapping

This tool combines the functionality of
[ip2asn](https://github.com/romain-fontugne/ip2asn) (IP-to-ASN mapping based on RIBs
collected by Route Views) with the knowledge of [PeeringDB](https://www.peeringdb.com/)
and [Alice-LG](https://github.com/alice-lg/alice-lg) looking glasses to provide
additional information about IXP nodes.

It uses the `ix` and `netixlan` API endpoints for information about IXP peering
LAN prefixes as well as IPs of individual IXP participants. This information
enables mapping to IXPs (using peering LAN prefixes) and additional ASN mappings
(using participant IPs).

The PeeringDB information can be obtained from dumps created by `fetch-pdb-dump.py`.

Looking glass information is obtained by running `fetch-lg-dumps.py`.

## Installation

Clone the repository:

```bash
git clone git@github.com:m-appel/iplookup.git
```

Install required Python dependencies:

```bash
pip install -r requirements.txt
```

## Usage

The constructor takes two parameters:

1. A `configparser.ConfigParser` instance containing required configuration options.
1. An optional timestamp from which to start reading the Kafka topics (if applicable).

This repository does not include [ip2asn](https://github.com/romain-fontugne/ip2asn) as
a submodule because it has grown quite large. Instead, you have to specify the path to
an existing installation instead. In addition, the location of `ix` and `netixlan` files
need to be specified.

### Example configuration

```ini
[ip2asn]
path = path/to/ip2asn
# Optional, defaults to path/db/latest.pickle
db = path/to/rib.pickle.bz2

[ip2ixp]
ix_file = pdb_dumps/pdb.ix.latest.pickle.bz2
netixlan_file = pdb_dumps/pdb.netixlan.latest.pickle.bz2
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
