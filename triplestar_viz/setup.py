from setuptools import find_packages, setup

package_name = 'triplestar_viz'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        (
            'share/ament_index/resource_index/packages',
            ['resource/' + package_name],
        ),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=[
        'setuptools',
        'graphviz',
        'pyoxigraph',
        'Pillow',
    ],
    zip_safe=True,
    maintainer='marijn',
    maintainer_email='derijkmarijn00@gmail.com',
    description='TODO: Package description',
    license='TODO: License declaration',
    extras_require={
        'ros': [
            'rclpy',
            'tf2_ros',
            'triplestar_core',
        ],
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'console_scripts': [
            'kb_visualizer_node = triplestar_viz.kb_visualizer_node:main',
            'rdfstar_viz = triplestar_viz.cli:main',
        ],
    },
)
