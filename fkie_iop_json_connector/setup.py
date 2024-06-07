from distutils.core import setup # find_packages

package_name = 'fkie_iop_json_connector'

setup(
    name=package_name,
    version='0.1.0',
    license='MIT',
    maintainer='Alexander Tiderko',
    maintainer_email='Alexander.Tiderko@fkie.fraunhofer.de',
    description='IOP JSON Connector - Translates IOP Messages to/from JSON.',
    url='https://github.com/FFI-no/iop-json-connector',
    # packages=find_packages(),
    packages=[package_name, package_name + '.transport', package_name + '.schemes'],
    data_files=[
        ('share/ament_index/resource_index/packages',
         ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    package_data={"": ["*.json"]},
    include_package_data=True,
    install_requires=['simple-websocket-server', 'ruamel.yaml'],
    zip_safe=True,
    tests_require=['pytest'],
    test_suite="tests",
    # entry_points={
    #     'console_scripts': [
    #         'ros-iop-json-connector.py ='
    #         ' fkie_iop_json_connector:main',
    #     ],
    # },
    scripts=['scripts/iop-json-connector.py', 'scripts/ros-iop-json-connector.py']
)
