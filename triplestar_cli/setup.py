from setuptools import find_packages
from setuptools import setup

package_name = 'triplestar_cli'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='marijn',
    maintainer_email='derijkmarijn00@gmail.com',
    description='CLI extension for interacting with triplestar knowledge base services.',
    license='Apache-2.0',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'ros2cli.command': [
            'triplestar = triplestar_cli.command.triplestar:TriplestarCommand',
        ],
        'ros2cli.extension_point': [
            'triplestar.verb = triplestar_cli.verb:VerbExtension',
            'triplestar.query.verb = triplestar_cli.query:VerbExtension',
            'triplestar.bringup.verb = triplestar_cli.bringup:VerbExtension',
        ],
        'triplestar.verb': [
            'query = triplestar_cli.verb.query:QueryVerb',
            'bringup = triplestar_cli.verb.bringup:BringupVerb',
            'start = triplestar_cli.verb.lifecycle:StartVerb',
            'stop = triplestar_cli.verb.lifecycle:StopVerb',
            'status = triplestar_cli.verb.status:StatusVerb',
        ],
        'triplestar.query.verb': [
            'list = triplestar_cli.query.list:ListVerb',
            'call = triplestar_cli.query.call:CallVerb',
            'info = triplestar_cli.query.info:InfoVerb',
        ],
        'triplestar.bringup.verb': [
            'new = triplestar_cli.bringup.new:NewVerb',
            'launch = triplestar_cli.bringup.launch:LaunchVerb',
            'list = triplestar_cli.bringup.list:ListVerb',
        ],
    },
)
