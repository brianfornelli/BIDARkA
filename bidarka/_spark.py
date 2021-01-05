from _util import read_config, script_path
import subprocess
import logging
from string import Template
import json

logging.basicConfig(format='%(message)s', level=logging.INFO)


def spark(path, payload={}, configuration=None):
    config = read_config()
    if configuration is None:
        spark_config = config.SPARK_CONFIG
    else:
        spark_config = configuration
    template = _read_template()
    spark_submit = spark_config["spark_submit"]
    spark_master = spark_config["spark_master"]
    properties = " ".join(spark_config["spark_properties"])
    conf = " ".join(["--conf {}".format(c) for c in spark_config["spark_conf"]])
    bucket = dict(spark_submit=spark_submit, spark_master=spark_master, spark_properties=properties,
                  spark_conf=conf, script_path=path)
    command = Template(template)
    result = command.substitute(bucket)
    script = result.split()
    script.append("--config={}".format(json.dumps(payload)))
    logging.info(" ".join(script))
    subprocess.check_call(script)


def _read_template():
    path = script_path("templates/spark.template")
    with open(path, 'r') as f:
        src = f.read()
    return src


if __name__ == '__main__':
    path = script_path("scripts/spark_test.py")
    spark(path, payload=dict(test="This is a test"))
