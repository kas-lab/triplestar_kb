from setuptools import find_packages, setup

package_name = 'triplestar_core'
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
    ],
    install_requires=[
        'returns>=0.22.0',
        'shapely',
        'pyoxigraph',
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
        ],
    },
)
