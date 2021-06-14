#!/usr/bin/python3 
import argparse
import csv
import datetime
import json
import logging
import re

import bcrypt
from kafka import KafkaProducer


class KafkaUtils():
    """Helpers from kafka."""


    @staticmethod
    def get_producer(conf):
        """Create a kafka producer according to conf

        :param conf: Configuration dict
        :type conf: dict
        :return: Producer
        :rtype: KafkaProducer
        """
        return KafkaProducer(
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            bootstrap_servers=conf["bootstrap_servers"],
            ssl_cafile=None
                if "ssl" not in conf
                else conf["ssl"]["ssl_cafile"],
            ssl_check_hostname=True
                if "ssl" not in conf
                else conf["ssl"]["ssl_check_hostname"],
            ssl_certfile=None
                if  "ssl" not in conf or \
                    "ssl_certfile" not in conf["ssl"]
                else conf["ssl"]["ssl_certfile"],
            ssl_keyfile=None
                if  "ssl" not in conf or \
                    "ssl_keyfile" not in conf["ssl"]
                else conf["ssl"]["ssl_keyfile"],
            ssl_password=None
                if  "ssl" not in conf or \
                    "ssl_password" not in conf["ssl"]
                else conf["ssl"]["ssl_password"],
            batch_size=16384*16,
            sasl_plain_username=KafkaUtils.get_cred(
                conf, "username"
            ),
            sasl_plain_password=KafkaUtils.get_cred(
                conf,  "password"
            ),
            security_protocol=KafkaUtils.get_security_protocol(
                conf
            ),
            sasl_mechanism='PLAIN'
        )

    @staticmethod
    def get_cred(conf, cred_name):
        """Get a cred (username or password)

        Args:
            cred_name (string): "username" or "password"
        """

        if cred_name not in conf or \
            not conf[cred_name]:
            cred = None
        else:
            cred = conf[cred_name]

        return cred

    @staticmethod
    def get_security_protocol(conf):
        """Get security protocol

        Args:
            conf (dict): kafka config
        """
        sec_protocol="PLAINTEXT"
        if "ssl" in conf:
            if "password" in conf and \
                conf["password"]:
                sec_protocol="SASL_SSL"
            else:
                sec_protocol="SSL"
        elif "password" in conf and \
                conf["password"]:
            sec_protocol="SASL_PLAINTEXT"
        return sec_protocol



def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-csv',
        type=str,
        required=True,
        help="csv file"
    )
    parser.add_argument(
        '-producer',
        type=str,
        required=True,
        help="Producer ID"
    )
    parser.add_argument(
        '-bootstrap',
        type=str,
        required=True,
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        '-topic',
        type=str,
        required=True,
        help="Target topic"
    )
    parser.add_argument(
        '-ca',
        type=str,
        required=False,
        help="Kafka CA file"
    )
    parser.add_argument(
        '-cert',
        type=str,
        required=False,
        help="Kafka user's cert"
    )
    parser.add_argument(
        '-cert-pass',
        type=str,
        required=False,
        help="Kafka cert's password"
    )
    parser.add_argument(
        '-anonymize',
        type=str,
        required=False,
        help="Optional: Anonymization config file"
    )

    ret = parser.parse_args()

    if (ret.ca or ret.cert or ret.cert_pass) \
        and \
        (ret.ca is None or ret.cert is None or ret.cert_pass is None):
        raise Exception("ca, cert and ca-pass should be set all togethers")
    
    return ret

def load_anonymise_config(filename):

    if not filename:
        return {}
    
    with open(filename) as json_file:
        return json.load(json_file)


def load_csv(filename, producer_id, kafka_producer, topic, bcrypt_conf):
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            payload = {
                "event_date": datetime.datetime.utcnow().replace(
                    tzinfo=datetime.timezone.utc
                ).isoformat(),
                "producer": producer_id
            }
            for key in row:
                if key in bcrypt_conf:
                    val = bcrypt.hashpw(
                        row[key].encode(),
                        bcrypt_conf[key].encode()
                    ).decode()
                else:
                    val = row[key]
                
                payload[key] = val

            kafka_producer.send(
                topic,
                value=payload,
                key=payload["user_id"].encode() if "user_id" in payload else payload["event_date"].encode()
            )
            print(payload)
                
def get_kafka_producer(config):
    kafka_conf = {
        "bootstrap_servers": config.bootstrap
    }
    if config.ca is not None:
        kafka_conf["ssl"] = {
            "ssl_cafile": config.ca,
            "ssl_keyfile": config.cert,
            "ssl_password": config.cert_pass
        }

    return KafkaUtils.get_producer(kafka_conf)

def main():

    logging.basicConfig(level=logging.INFO)

    config = parse_args()

    load_csv(
        config.csv,
        config.producer,
        get_kafka_producer(config),
        config.topic,
        load_anonymise_config(config.anonymize)
    )

if __name__ == "__main__":
    main()