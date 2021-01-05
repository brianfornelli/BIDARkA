from ._util import read_settings, read_template, template_transform, apply_file_template
from ._credentials import get_dir
import subprocess
import logging
import tempfile

logging.basicConfig(format='%(message)s', level=logging.INFO)


def beeline(command, payload={}) -> None:
    settings = read_settings()
    config = settings.BEELINE_CONFIG
    config.update(dict(flag='-e', command='"{}"'.format(command.replace("\r", " ").replace("\n", " "))))
    src = read_template('templates/beeline.template').split()
    script = [template_transform(t, config, payload) for t in src]
    logging.info(" ".join(script))
    subprocess.check_call(script)


def hive_hql(path, payload={}) -> None:
    settings = read_settings()
    config = settings.BEELINE_CONFIG
    dirpath = get_dir()
    script_text = apply_file_template(path, payload)
    with tempfile.NamedTemporaryFile(suffix=".hql", dir=dirpath) as f:
        f.write(script_text.encode('utf-8'))
        f.flush()
        config.update(dict(flag='-f', command=f.name))
        src = read_template('templates/beeline.template').split()
        script = [template_transform(t, config, payload) for t in src]
        logging.info(" ".join(script))
        subprocess.check_call(script)
