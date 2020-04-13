PROJECT = 'ocli-core'

# Change docs/sphinx/conf.py too!
VERSION = '0.2.5'
from setuptools import setup, find_namespace_packages

setup(
    name=PROJECT,
    version=VERSION,

    description='Open Source components of the OPT/NET awards winning TSAR-AI EoD analytics platform',
    long_description="""The newest addition to OptOSS AI suite is TSAR AI developed specifically for processing of Copernicus Earth Observations Data. This repository contains the Open Source elements of TSAR AI only and is provided for intended audiences of researches and scientists in the field of remote sensing.
This open source version of TSAR AI platform makes human analysts aware of changes to water bodies and on the surface. TSAR AI combines Earth observation data from multiple bands and then AI produces delineation maps. With growing fleet of Earth observation satellites which use Synthetic Aperture Radar antenna (SAR) as a remote sensing tool more frequent revisit times allow for almost daily reports with valuable insights being generated automatically in fraction of the time when compared with human analysts.
This open source version relies on the ESA Sentinel Toolbox SNAP for processing of the satellite imagery with provided graph via GTP. See the Readme file for more information.
TSAR AI won the 2018 EC Copernicus Masters Emergency Management Challenge and was further developed by combining public and private money, e.g. EC Copernicus Incubation grant and angel investor funding.
TSAR AI project is developed and maintained by OPT/NET BV (http://opt-net.eu) – a Dutch startup who makes and markets the high performance and high precision commercial modules for TSAR AI platform as well, with multiple mapping modules produced for emergency management and smart agriculture application areas.
""",

    author ='OPT/NET BV',
    author_email='info@opt-net.eu',

    url='https://github.com/Opt-OSS/TSAR-AI',
    download_url='https://github.com/Opt-OSS/TSAR-AI',

    classifiers=['Development Status :: 4 - Beta',
                 'License :: OSI Approved :: Apache Software License',
                 'Programming Language :: Python :: 3.7',
                 'Intended Audience :: Science/Research',
                 'Environment :: Console',
                 ],

    platforms=['Any'],

    scripts=[],

    provides=[],
    install_requires=[
        'gdal>=3',
        'scipy',
        'scikit-learn',
        # 'pyproj<2', # Seg-fault with >= 2
        'click',
        'click_repl',
        'urllib3<1.25,>=1.20',
        'tabulate',
        'pyyaml',
        'prompt_toolkit>=2.0',
        'geopandas',
        'cachetools',
        'coloredlogs',
        'requests',
        # 'ibm_cos_sdk>=2.4.4',
        'boto3',
        'jsonschema>=3',
        'spectral',
        'scikit-image',
        'tqdm',
        'pygments',
        'cartopy',
        'affine',
        'python-slugify',
        'json-sempai',
        'descartes',
        'dateparser ',
        'lxml',
        'dpath',
        'mgrs'
    ],

    namespace_packages=[],
    packages=find_namespace_packages(include=['ocli.*']),
    include_package_data=True,

    entry_points={
        'console_scripts': [
            'ocli = ocli.cli.cli:loop'
        ],
    },
    data_files=[('ocli/ai/',
                 ['ocli/ai/recipe_schema.json',
                  'ocli/ai/recipe_schema_rvi.json']),
                ('ocli/cli/scripts/',
                 ['ocli/cli/scripts/rsync-metadata.sh', 'ocli/cli/scripts/Sig-Coh-Stack-VH-VV-FIN-orb.xml']),
                ],
    zip_safe=False,
)
