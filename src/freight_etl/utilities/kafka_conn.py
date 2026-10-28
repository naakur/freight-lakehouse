"""
Kafka connection options for Confluent Cloud (SASL_SSL + PLAIN).

Credentials are NEVER written in code or YAML. They live in a Databricks secret scope,
one secret per value (this maps 1:1 to a Key Vault-backed scope on Azure):
    kafka-bootstrap   kafka-api-key   kafka-api-secret
"""


def kafka_options(get_secret) -> dict:
    """
    get_secret: a function that returns a secret by key name, e.g.
                lambda k: dbutils.secrets.get("freight-kafka", k)
    """
    bootstrap = get_secret("kafka-bootstrap")
    api_key = get_secret("kafka-api-key")
    api_secret = get_secret("kafka-api-secret")

    # On Databricks the Kafka client classes are "shaded", hence the long prefix.
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
    }
