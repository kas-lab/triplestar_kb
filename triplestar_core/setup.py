import os

from setuptools import find_packages
from setuptools import setup

package_name = 'triplestar_core'


def collect_template_files():
    """
    Collect all files under bringup_template/ for install into share/.

    Returns a list of (install_dir, [source_files]) tuples suitable for
    setuptools' data_files argument.
    """
    data_files = []
    template_root = 'bringup_template'
    for dirpath, _, filenames in os.walk(template_root):
        files = [os.path.join(dirpath, filename) for filename in filenames]
        if not files:
            continue
        install_dir = os.path.join('share', package_name, dirpath)
        data_files.append((install_dir, files))
    return data_files


setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test', 'scripts', 'resource']),
    data_files=[
        (
            'share/ament_index/resource_index/packages',
            ['resource/' + package_name],
        ),
        ('share/' + package_name, ['package.xml']),
        *collect_template_files(),
    ],
    install_requires=[
        'shapely',
        'pyoxigraph',
        'reasonable',
        'oxrdflib',
        'pydantic',
        'jinja2',
        'pyyaml',
        'copier',
    ],
    zip_safe=True,
    maintainer='marijn',
    maintainer_email='derijkmarijn00@gmail.com',
    description='TODO: Package description',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'kb_node = triplestar_core.kb_node:main',
            'kb_marker_publisher = triplestar_core.kb_marker_publisher:main',
            'query_kb = scripts.query_kb:main',
            'new_bringup = triplestar_core.new_bringup:main',
        ],
    },
)
