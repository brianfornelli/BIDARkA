from ._util import read_settings, read_template, template_transform, set_env_variables
from ._const import ENV_VARIABLES
import json
import subprocess
import logging


def spark(path, payload={}, configuration=None):
    settings = read_settings()
    spark_config = settings.SPARK_CONFIG
    if configuration is not None:
        spark_config.update(configuration)

    if ENV_VARIABLES in spark_config.keys():
        set_env_variables(spark_config[ENV_VARIABLES])

    template = read_template("templates/spark.template")
    spark_submit = spark_config["spark_submit"]
    spark_master = spark_config["spark_master"]
    properties = " ".join(spark_config["spark_properties"])
    conf = " ".join(["--conf {}".format(c) for c in spark_config["spark_conf"]])
    bucket = dict(spark_submit=spark_submit, spark_master=spark_master, spark_properties=properties,
                  spark_conf=conf, script_path=path)
    command = template_transform(template, bucket)
    script = command.split()
    script.append("--config={}".format(json.dumps(payload)))
    logging.info(" ".join(script))
    subprocess.check_call(script)
