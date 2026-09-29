"""Checksummed gzip JSON packs. Encoding v1 preserves Python JSON numeric values."""
import gzip
import hashlib
import json

CODEC = 'json-gzip-v1'
MAX_DECODED_BYTES = 256 * 1024 * 1024


class ArchiveError(ValueError):
    pass


def encode(kind, source, rows):
    document = {'version': 1, 'kind': kind, 'source_updated_ms': source, 'rows': rows}
    raw = json.dumps(document, separators=(',', ':'), allow_nan=False).encode()
    if len(raw) > MAX_DECODED_BYTES:
        raise ArchiveError('archive exceeds decoded size limit')
    payload = gzip.compress(raw, compresslevel=6, mtime=0)
    return payload, hashlib.sha256(payload).hexdigest()


def decode(payload, checksum, kind, source):
    if hashlib.sha256(payload).hexdigest() != checksum:
        raise ArchiveError('archive checksum mismatch')
    try:
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
            raw = stream.read(MAX_DECODED_BYTES + 1)
        if len(raw) > MAX_DECODED_BYTES:
            raise ArchiveError('archive exceeds decoded size limit')
        document = json.loads(raw)
        if (document['version'] != 1 or document['kind'] != kind
                or document['source_updated_ms'] != source or not isinstance(document['rows'], list)):
            raise ArchiveError('archive version, kind or source mismatch')
        return document['rows']
    except (OSError, EOFError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ArchiveError('malformed archive') from exc
