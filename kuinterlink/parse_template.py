from pathlib import Path
import os.path
import jinja2
import yaml

from typing import Literal

AvailableTemplate = Literal['Job']

def parse_template(template_name: AvailableTemplate, **kwargs):
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    print (template_path)
    jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))
    template = jinja_env.get_template(f"{template_name}.yaml")
    parsed_yaml = template.render(**kwargs)
    return yaml.safe_load(parsed_yaml)




if __name__ == '__main__':
    from pprint import pprint

    job_example = parse_template('Job', job=dict(
        metadata=dict(name="pippo", namespace="pluto", queue="paperino"),
        containers=[
            dict(
                name='container-pippo',
                image='python',
                tag='latest',
                command=["echo"],
                args=["ahhh"],
                resources=dict(
                    limits=dict(
                        cpu=1,
                        memory="1Gi")
                ),
                volumeMounts=[
                    dict(name='pippo', mountPath='/pippo', subPath='/subpippo'),
                    dict(name='pluto', mountPath='/pluto'),
                ]
            ),
        ],
        initContainers=[
            dict(
                name='container-pippo',
                image='python',
                tag='latest',
                command=["echo"],
                args=["ahhh"],
                resources=dict(
                    limits=dict(
                        cpu=1,
                        memory="1Gi")
                ),
                volumeMounts=[
                    dict(name='pippo', mountPath='/pippo', subPath='/subpippo'),
                    dict(name='pluto', mountPath='/pluto'),
                ]
            ),
        ],

    ))

    pprint (job_example)