'''
fork of https://github.com/bunnis/esa_sentinel
'''
# TODO rewrite it
import itertools
import logging
from pprint import pprint
from xml.etree.ElementTree import Element

import lxml
from lxml import etree
import os
import re

log = logging.getLogger()


supported_raw = [
    r'S1[AB]_IW_RAW__0SSV',
    r'S1[AB]_IW_RAW__0SSH',
    r'S1[AB]_IW_RAW__0SDV',
    r'S1[AB]_IW_RAW__0SDH',
    r'S1[AB]_EW_RAW__0SDH',
    r'S1[AB]_EW_RAW__0SSH',
    r'S1[AB]_EW_RAW__0SDV',
    r'S1[AB]_S1_RAW__0SSV',
    r'S1[AB]_S1_RAW__0SDH',
    r'S1[AB]_S5_RAW__0SSV',
    r'S1[AB]_S3_RAW__0SDV',
    r'S1[AB]_S3_RAW__0SSV',
    r'S1[AB]_S3_RAW__0SDH',
    r'S1[AB]_S4_RAW__0SSV',
    r'S1[AB]_S6_RAW__0SSV',
]
supported_gr = [
    r'S1[AB]_S3_GRDH_1SDH',
    r'S1[AB]_IW_SLC__1SSV',
    r'S1[AB]_IW__1SSH',
    r'S1[AB]_IW_SLC__1SDV',
    r'S1[AB]_IW_SLC__1SDH',
    r'S1[AB]_IW_GRDH_1SSV',
    r'S1[AB]_IW_GRDH_1SSH',
    r'S1[AB]_IW_GRDH_1SDV',
    r'S1[AB]_IW_GRDH_1SDH',
    r'S1[AB]_EW_GRDM_1SSH',
    r'S1[AB]_EW_GRDM_1SDV',
    r'S1[AB]_EW_GRDH_1SDH',
    r'S1[AB]_EW_GRDM_1SDH',
    r'S1[AB]_IW_SLC__1SSH',
    r'S1[AB]_S5_GRDH_1SSV',
    r'S1[AB]_EW_GRDH_1SSH',
    r'S1[AB]_S5_SLC__1SSV',
    r'S1[AB]_IW_GRDH_1SDV',
    r'S1[AB]_S4_GRDH_1SSV',
    r'S1[AB]_S3_GRDH_1SSV',
    r'S1[AB]_S3_GRDH_1SDV',
    r'S1[AB]_S4_SLC__1SSV',
    r'S1[AB]_S3_SLC__1SSV',
    r'S1[AB]_S1_GRDH_1SDH',
    r'S1[AB]_S3_SLC__1SDV']

supported_ocn = [
    r'S1[AB]_IW_OCN__2SDV',
    r'S1[AB]_WV_OCN__2SSV',
    r'S1[AB]_IW_OCN__2SSV',
    r'S1[AB]_EW_OCN__2SDH',
    r'S1[AB]_IW_OCN__2SDH'
]

supported_s2 = [r'S2[AB]_OPER_PRD_MSIL1C_PDMC']
supported_s2_msil1c = [
    r'S2[AB]_MSIL1C'
]

supported_sentinel_formats =  list(itertools.chain(supported_raw ,
                                                   supported_gr ,
                                                   supported_ocn ,
                                                   supported_s2 ,
                                                   supported_s2_msil1c))


class SentinelMetadataExtractor:
    filepath = ''
    tree = ""
    root: Element = None
    file_error_count = 0
    filenames_error = []
    total_files = 0
    productMetadata = {}
    productMetadataEtrees = {}

    def __init__(self):
        pass

    def extractMetadataFromManifestFiles(self, filename):
        '''main method for metadata extarction'''

        #####all this names represent files succesfully parsed with this program

        processed = False
        for sentinel_name in supported_gr:
            if re.match(sentinel_name, filename):
                self.productMetadata = self._extractGR()
                processed = True

        for sentinel_name in supported_raw:
            if re.match(sentinel_name, filename):
                self.productMetadata = self._extractRAW()
                processed = True

        for sentinel_name in supported_s2_msil1c:
            if re.match(sentinel_name, filename):
                self.productMetadata = self._extractS2MSIL1C()
                processed = True
        for sentinel_name in supported_s2:
            if re.match(sentinel_name, filename):
                self.productMetadata = self._extractS2()
                processed = True
        for sentinel_name in supported_ocn:
            if re.match(sentinel_name, filename):
                self.productMetadata = self._extractIWOCN()
                processed = True
        if processed:
            # transform coordinates to geojson geometry
            pass
        else:
            self.file_error_count = self.file_error_count + 1
            self.filenames_error.append(str(filename))
            raise AssertionError("FILE NOT IN KNOWN FILES - " + str(filename))

    def _transformSolrCoordsToSAFECoords(self, coords):
        '''receives coordinates in solr format and parses to the one in SAFE format for further processing
        FROM #POLYGON ((123.3108 8.0611,123.0589 9.3117,120.8469 9.0181,121.1061 7.7648,123.3108 8.0611,123.3108 8.0611))
        TO 8.0611,123.3108 9.3117,123.0589 9.0181,120.8469 7.7648,121.1061 8.0611,123.3108
        '''

        # remove (( and )), split by commas
        regexed = re.findall("POLYGON\s\D\D(.*)\D\D", coords)[0]  # [0], grab first (and only) group
        coords_split = regexed.split(',')
        # coords_split = str(re.findall("POLYGON\s\D\D(.*)\D\D", coords)).split(',')

        # add commas in whitespaces, and a whitespace to separate each pair, result is something like this 8.0611,123.3108 9.3117,123.0589 9.0181,120.8469 7.7648,121.1061 8.0611,123.3108
        coords_united = ''
        for c in range(0, len(coords_split)):
            replaced = coords_split[c].replace(" ", ",")

            coords_united = coords_united + replaced
            if c != (len(coords_split) - 1):
                coords_united = coords_united + " "

        return str(coords_united)

    def _parseCoordinates(self, coordinates):
        '''parse coordinates to a json format[[[lat1,long1],[lat2,long2],...]]'''
        # in '58.893013,-65.056816 59.638775,-57.844292 55.804028,-56.761806 55.089973,-63.272209'
        # out [['58.893013', '-65.056816'], ['59.638775', '-57.844292'], ['55.804028', '-56.761806'], ['55.089973', '-63.272209']]
        final_list = []
        coordsx = []  # lat
        coordsy = []  # long
        split = re.split(' |,', coordinates.strip())
        # print split

        for c in range(0, len(split)):
            if c % 2 == 0:  # pair
                coordsx.append(split[c])
            else:
                coordsy.append(split[c])

        for c in range(0, len(coordsx)):
            # pprint(coordsx[c])
            final_list.append([float(coordsy[c]), float(coordsx[c])])
        #close poligon
        final_list.append([float(coordsy[0]), float(coordsx[0])])
        # pprint( final_list)
        return final_list  # lat,long

    def _extractS2(self):
        log.info(f"Manifest resolved as S2 Common")
        metadata = {}
        ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
        ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
        ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',
            self.root.nsmap)
        metadata['StartTime'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName', self.root.nsmap)
        metadata['FamilyName'] = extracted[0].text

        extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',
                                      self.root.nsmap)
        metadata['FamilyNameNumber'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',
            self.root.nsmap)
        metadata['InstrumentFamilyName'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel/1.1}frameSet/{http://www.esa.int/safe/sentinel/1.1}footPrint/{http://www.opengis.net/gml}coordinates',
            self.root.nsmap)
        metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
        #
        return metadata

    def _extractS2MSIL1C(self):
        log.info(f"Manifest resolved as MSIL1C")
        metadata = {}
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',
            self.root.nsmap)
        metadata['StartTime'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName', self.root.nsmap)
        metadata['FamilyName'] = extracted[0].text

        extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',
                                      self.root.nsmap)
        metadata['FamilyNameNumber'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',
            self.root.nsmap)
        metadata['InstrumentFamilyName'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel/1.1}frameSet/{http://www.esa.int/safe/sentinel/1.1}footPrint/{http://www.opengis.net/gml}coordinates',
            self.root.nsmap)
        metadata['Coordinates'] = []
        # metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
        #
        return metadata

    def _extractGR(self):
        log.info(f"Manifest resolved as GR")
        metadata = {}
        ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
        ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
        ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',
            self.root.nsmap)
        metadata['StartTime'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',
            self.root.nsmap)
        metadata['StopTime'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName', self.root.nsmap)
        metadata['FamilyName'] = extracted[0].text

        extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',
                                      self.root.nsmap)
        metadata['FamilyNameNumber'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',
            self.root.nsmap)
        metadata['InstrumentFamilyName'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl1:instrumentMode/s1sarl1:mode',
            self.root.nsmap)
        metadata['InstrumentMode'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClass',
            self.root.nsmap)
        metadata['ProductClass'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClassDescription',
            self.root.nsmap)
        metadata['ProductClassDescription'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productComposition',
            self.root.nsmap)
        metadata['ProductComposition'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productType',
            self.root.nsmap)
        metadata['ProductType'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:transmitterReceiverPolarisation',
            self.root.nsmap)
        if len(extracted) > 1:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
        else:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',
            self.root.nsmap)
        metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)

        extracted = self.root.find(
            "./metadataSection/metadataObject/metadataWrap/xmlData/safe:orbitReference/safe:relativeOrbitNumber[@type='start']",
            self.root.nsmap)
        metadata['relativeOrbitNumber'] = extracted.text
        #
        return metadata

    def _extractRAW(self):
        log.info(f"Manifest resolved as RAW")
        metadata = {}
        ###############S1A_S3_RAW__0SDH###############S1A_IW_RAW__0SSV###############S1A_IW_RAW__0SSH###############S1A_IW_RAW__0SDV
        ###############S1A_IW_RAW__0SDH###############S1A_EW_RAW__0SDH
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}startTime',
            self.root.nsmap)
        metadata['StartTime'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}stopTime',
            self.root.nsmap)
        metadata['StopTime'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}familyName',
            self.root.nsmap)
        metadata['FamilyName'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}number',
            self.root.nsmap)
        metadata['FamilyNameNumber'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}familyName',
            self.root.nsmap)
        metadata['InstrumentFamilyName'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}extension/s1sar:instrumentMode/s1sar:mode',
            self.root.nsmap)
        metadata['InstrumentMode'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClass',
            self.root.nsmap)
        metadata['ProductClass'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClassDescription',
            self.root.nsmap)
        metadata['ProductClassDescription'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productConsolidation',
            self.root.nsmap)
        metadata['ProductConsolidation'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:transmitterReceiverPolarisation',
            self.root.nsmap)
        if len(extracted) > 1:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
        else:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}frameSet/{http://www.esa.int/safe/sentinel-1.0}frame/{http://www.esa.int/safe/sentinel-1.0}footPrint/{http://www.opengis.net/gml}coordinates',
            self.root.nsmap)
        metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)

        return metadata

    def _extractIWOCN(self):
        log.info(f"Manifest resolved as IWOCN")
        metadata = {}
        ###############S1A_IW_OCN__2SDV
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',
            self.root.nsmap)
        metadata['StartTime'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',
            self.root.nsmap)
        metadata['StopTime'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName', self.root.nsmap)
        metadata['FamilyName'] = extracted[0].text

        extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',
                                      self.root.nsmap)
        metadata['FamilyNameNumber'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',
            self.root.nsmap)
        metadata['InstrumentFamilyName'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl2:instrumentMode/s1sarl2:mode',
            self.root.nsmap)
        metadata['InstrumentMode'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClass',
            self.root.nsmap)
        metadata['ProductClass'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClassDescription',
            self.root.nsmap)
        metadata['ProductClassDescription'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productComposition',
            self.root.nsmap)
        metadata['ProductComposition'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productType',
            self.root.nsmap)
        metadata['ProductType'] = extracted[0].text

        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:transmitterReceiverPolarisation',
            self.root.nsmap)
        if len(extracted) > 1:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
        else:
            metadata['TransmitterReceiverPolarisation'] = extracted[0].text
        #
        extracted = self.root.findall(
            './metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',
            self.root.nsmap)
        metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)

        return metadata

    def getProductsMetadata(self):
        '''return all ingested products metadata
        the actual metadata, dict in which keys are the filenames in lowercase, values are a dict of keys,values
        '''
        return self.productMetadata

    def _getBoundingBox(self, coords):
        '''returns a bounding box for a given set of coordinates
        '''
        ##parse coords
        coordsx = []  # lat
        coordsy = []  # long

        for c in range(0, len(coords)):
            coordsx.append(coords[c][0])
            coordsy.append(coords[c][1])

        xmax = max(coordsx)
        xmin = min(coordsx)
        ymax = max(coordsy)
        ymin = min(coordsy)

        # lat max = north bound lat
        # lat min = south bound lat
        # long max = east bound long
        # ong min = west bound long
        return [xmax, xmin, ymax, ymin]

    def _folderExists(self, outputFolder):
        '''check if path exists and then check if path given is a folder, returns boolean'''
        if not os.path.exists(outputFolder):
            print('Folder does not exist, creating ' + outputFolder + ' .')
            try:
                os.makedirs(outputFolder)
                return True
            except:
                print('Folder creation not succesfull, maybe you do not have permissions')
                return False

        try:
            os.path.isdir(outputFolder)
            return True
        except:
            print('given outputFolder path is not a folder')
            return False

    def generateInspireFromTemplate(self, metadata_key, template='inspire_template.xml',
                                    outputFolder='/tmp/harvested/manifests-inspire/', writeToFile=False):
        '''glued with spit and hammered code to generate inspire xml based on a custom template
        general idea is to replace the values on the template with the ones from our metadata
        check http://inspire-geoportal.ec.europa.eu/editor/
        returns the etree
        outputFolder can be empty if writeToFile is false
        '''

        # print metadata_key

        if writeToFile:  # if folder doesnt exist and we want to write to file, exit
            if not self._folderExists(outputFolder):
                return None

        if metadata_key in self.productMetadata.keys():
            try:
                template_tree = etree.parse(template)
                template_root = template_tree.getroot()

                current_metadata = self.productMetadata[metadata_key]

                # uuid
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}fileIdentifier/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = current_metadata['uuid']

                # org name
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}contact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}organisationName/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'ESA'

                # org contact email
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}contact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}contactInfo/{http://www.isotc211.org/2005/gmd}CI_Contact/{http://www.isotc211.org/2005/gmd}address/{http://www.isotc211.org/2005/gmd}CI_Address/{http://www.isotc211.org/2005/gmd}electronicMailAddress/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'esapub@esa.int'

                # date created
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}dateStamp/{http://www.isotc211.org/2005/gco}Date',
                    template_root.nsmap)
                find[0].text = current_metadata['StartTime'][:10]

                # filename
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}title/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = metadata_key.upper()

                # date for metadata creation (this xml)
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}date/{http://www.isotc211.org/2005/gmd}CI_Date/{http://www.isotc211.org/2005/gmd}date/{http://www.isotc211.org/2005/gco}Date',
                    template_root.nsmap)
                find[0].text = current_metadata['StartTime'][:10]

                # uuid
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}identifier/{http://www.isotc211.org/2005/gmd}RS_Identifier/{http://www.isotc211.org/2005/gmd}code/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = current_metadata['uuid']

                # abstract
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}abstract/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'ESA Sentinel Product Metadata'

                # responsible org
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}pointOfContact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}organisationName/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'ESA'

                # responsible org contact
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}pointOfContact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}contactInfo/{http://www.isotc211.org/2005/gmd}CI_Contact/{http://www.isotc211.org/2005/gmd}address/{http://www.isotc211.org/2005/gmd}CI_Address/{http://www.isotc211.org/2005/gmd}electronicMailAddress/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'esapub@esa.int'

                # keywords from INSPIER Data Themes
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}descriptiveKeywords/{http://www.isotc211.org/2005/gmd}MD_Keywords/{http://www.isotc211.org/2005/gmd}keyword/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'Orthoimagery'

                # keywors from repositories
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}descriptiveKeywords/{http://www.isotc211.org/2005/gmd}MD_Keywords/{http://www.isotc211.org/2005/gmd}thesaurusName/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}title/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'GEMET - INSPIRE themes, version 1.0'

                # licenese conditions
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}resourceConstraints/{http://www.isotc211.org/2005/gmd}MD_Constraints/{http://www.isotc211.org/2005/gmd}useLimitation/{http://www.isotc211.org/2005/gco}CharacterString',
                    template_root.nsmap)
                find[0].text = 'Conditions unknown'

                # topic category code
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}topicCategory/{http://www.isotc211.org/2005/gmd}MD_TopicCategoryCode',
                    template_root.nsmap)
                find[0].text = 'imageryBaseMapsEarthCover'

                # start time of data
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}temporalElement/{http://www.isotc211.org/2005/gmd}EX_TemporalExtent/{http://www.isotc211.org/2005/gmd}extent/{http://www.opengis.net/gml}TimePeriod/{http://www.opengis.net/gml}beginPosition',
                    template_root.nsmap)
                find[0].text = current_metadata['StartTime'][:10]

                # coords
                bb = self._getBoundingBox(current_metadata['Coordinates'])
                # lat max = north bound lat
                # lat min = south bound lat
                # long max = east bound long
                # ong min = west bound long
                # return [xmax,xmin,ymax,ymin]

                # westBoundLongitude
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}westBoundLongitude/{http://www.isotc211.org/2005/gco}Decimal',
                    template_root.nsmap)
                find[0].text = bb[0]

                # eastBoundLongitude
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}eastBoundLongitude/{http://www.isotc211.org/2005/gco}Decimal',
                    template_root.nsmap)
                find[0].text = bb[1]

                # southBoundLatitude
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}southBoundLatitude/{http://www.isotc211.org/2005/gco}Decimal',
                    template_root.nsmap)
                find[0].text = bb[2]

                # northBoundLatitude
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}northBoundLatitude/{http://www.isotc211.org/2005/gco}Decimal',
                    template_root.nsmap)
                find[0].text = bb[3]

                # end time of data
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}temporalElement/{http://www.isotc211.org/2005/gmd}EX_TemporalExtent/{http://www.isotc211.org/2005/gmd}extent/{http://www.opengis.net/gml}TimePeriod/{http://www.opengis.net/gml}endPosition',
                    template_root.nsmap)
                if 'StopTime' in current_metadata.keys():  # sentinel 2 products sometimes dont have stoptime
                    find[0].text = current_metadata['StopTime'][:10]
                else:
                    find[0].text = current_metadata['StartTime'][:10]

                # link for the resource described in the metadata
                find = template_root.findall(
                    './{http://www.isotc211.org/2005/gmd}distributionInfo/{http://www.isotc211.org/2005/gmd}MD_Distribution/{http://www.isotc211.org/2005/gmd}transferOptions/{http://www.isotc211.org/2005/gmd}MD_DigitalTransferOptions/{http://www.isotc211.org/2005/gmd}onLine/{http://www.isotc211.org/2005/gmd}CI_OnlineResource/{http://www.isotc211.org/2005/gmd}linkage/{http://www.isotc211.org/2005/gmd}URL',
                    template_root.nsmap)
                find[0].text = current_metadata['downloadLink']

                if writeToFile:
                    output_filename = outputFolder + metadata_key.upper().split('.')[
                        0] + '.xml'  # remove .manifest.safe , add .xml
                    template_tree.write(output_filename, pretty_print=True)

                return template_tree

            except lxml.etree.XMLSyntaxError:
                print('File XML Syntax Error')
                return None

            # except Exception:
            # print 'Unspecified error occured, maybe the file doesn\'t exist'
            # return None

        else:
            print('Wrong metadata key')
            return None
