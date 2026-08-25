from glob import glob
import os

from setuptools import find_packages
from setuptools import setup

package_name = 'triplestar_bringup'


setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(),
    package_data={
        'triplestar_bringup': ['bringup_template/**', 'bringup_template/**/*'],
    },
    data_files=[
        (
            'share/ament_index/resource_index/packages',
            ['resource/' + package_name],
        ),
        (
            'share/' + package_name,
            ['package.xml'],
        ),
        (
            os.path.join('share', package_name, 'launch'),
            glob('launch/*'),
        ),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
)
