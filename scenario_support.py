"""Shared local-only fixtures and metric definitions."""
import math
import uuid
from contextlib import contextmanager
from lab import sqs_client

def percentile(values, percent):
    """Nearest-rank percentile; samples are complete per-message milliseconds."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)] if ordered else None


@contextmanager
def queues():
    client = sqs_client()
    prefix = 'qlab-advanced-' + uuid.uuid4().hex[:10]
    created = []

    def create(suffix, **attributes):
        url = client.create_queue(QueueName=prefix + '-' + suffix,
                                  Attributes={k: str(v) for k, v in attributes.items()})['QueueUrl']
        created.append(url)
        return url

    try:
        yield client, create, prefix
    finally:
        for url in reversed(created):
            client.delete_queue(QueueUrl=url)
        client.close()


def receive(client, url):
    return client.receive_message(QueueUrl=url, WaitTimeSeconds=1, MaxNumberOfMessages=1,
                                  MessageSystemAttributeNames=['All']).get('Messages', [])

