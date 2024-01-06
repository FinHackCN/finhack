from setuptools import setup, find_packages
import os 

root_dir = 'finhack'
version='0.0.1.dev5'

for subdir, dirs, files in os.walk(root_dir):
    if not '__init__.py' in files:
        init_file_path = os.path.join(subdir, '__init__.py')
        open(init_file_path, 'a').close()
        print(f'Created __init__.py in {subdir}')

with open('./finhack/__init__.py', 'w') as file:
    file.write(f"__version__ = '{version}'\n")


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='finhack',
    version=version,
    author='woldy',
    description='A scalable quantitative financial analysis framework.',
    packages=find_packages(),
    package_data={
        'finhack': ['*.*', '**/*.*']
    }, 
    include_package_data=True,
    scripts=['finhack/core/command/finhack'],
    install_requires=requirements, 
    )


