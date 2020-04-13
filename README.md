The newest addition to OptOSS AI suite is TSAR AI developed specifically for processing of Copernicus Earth Observations Data. This repository contains the Open Source elements of TSAR AI only and is provided for intended audiences of researches and scientists in the field of remote sensing.

This open source version of TSAR AI platform makes human analysts aware of changes to water bodies and on the surface. TSAR AI combines Earth observation data from multiple bands and then AI produces delineation maps. With growing fleet of Earth observation satellites which use Synthetic Aperture Radar antenna (SAR) as a remote sensing tool more frequent revisit times allow for almost daily reports with valuable insights being generated automatically in fraction of the time when compared with human analysts.

This open source version relies on the ESA Sentinel Toolbox SNAP for processing of the satellite imagery with provided graph via GTP. See the Readme file for more information.

TSAR AI won the 2018 EC Copernicus Masters Emergency Management Challenge and was further developed by combining public and private money, e.g. EC Copernicus Incubation grant and angel investor funding.

TSAR AI project is developed and maintained by OPT/NET BV (http://opt-net.eu) – a Dutch startup who makes and markets the high performance and high precision commercial modules for TSAR AI platform as well, with multiple mapping modules produced for emergency management and smart agriculture application areas.


# Install paltform
clone repository


```bash
git clone git@github.com:Opt-OSS/TSAR-AI.git
cd TSAR-AI
pip install .
```



# Install ESA SNAP

download [ESA SNAP](http://step.esa.int/main/download/snap-download/)

> if gpt is installed  localy, edit `.bashrc`  add `export PATH="/home/<USERNAME>/snap/bin:$PATH"`


`grp --diag`  - notice `SNAP home: /home/st/snap/bin/`

edit `gpt.vmoptions` `-Xmx60G` - set memory up to 90% (60 of 68 total here)

### libfortran snap issue
 
check `libfortran` version: 4.8.5 is required, 7.X is `not found` by SNAP

`yum install libgfortran`
notice lib-G-fortran


 
