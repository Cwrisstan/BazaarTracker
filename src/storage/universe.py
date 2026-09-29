import json
from pathlib import Path

DEFAULT = {'version': 1, 'policy_id': 'provisional-all-v1', 'mode': 'all', 'products': []}


def normalize(config):
    if not isinstance(config, dict) or set(config) != {'version', 'policy_id', 'mode', 'products'}:
        raise ValueError('universe needs exactly version, policy_id, mode, products')
    if type(config['version']) is not int or config['version'] != 1:
        raise ValueError('unsupported universe version')
    if not isinstance(config['policy_id'], str) or not config['policy_id'].strip():
        raise ValueError('universe policy_id must be nonempty')
    if config['mode'] not in ('all', 'include'):
        raise ValueError('universe mode must be all or include')
    products = config['products']
    if not isinstance(products, list) or any(not isinstance(p, str) or not p for p in products):
        raise ValueError('universe products must be an array of nonempty IDs')
    if len(set(products)) != len(products) or (config['mode'] == 'all' and products):
        raise ValueError('duplicate products or products supplied with all mode')
    return {**config, 'products': sorted(products)}


def load(path=None):
    return normalize(json.loads(Path(path).read_text()) if path else DEFAULT)


def selected(config, product):
    return config['mode'] == 'all' or product in config['products']
