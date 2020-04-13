import json
import logging
import os
from jsonschema import Draft7Validator, draft7_format_checker
import json
import logging
import os

from jsonschema import Draft7Validator, draft7_format_checker

ENV_RECIPE = 'RECIPE'  # name of environment variable with json string


class Recipe(dict):
    """Wrapper(Inherits from) around Dict object filled from recipe file
    """
    log = logging.getLogger("Recipe")

    def __init__(self, file):
        self.file = file
        fo = None
        try:
            if os.environ.get('RECIPE'):
                self.log.info("Getting recipe from environment $ENV_RECIPE")
                self.recipe = json.loads(os.environ.get('RECIPE'))
            elif isinstance(file,dict): #Mock recipe for tests
                self.recipe = self.file
            elif isinstance(file,str):
                    self.log.info(f"Getting recipe from file {file}")
                    fo = open(file, 'r')
                    self.recipe = json.load(fo)
            else :
                raise FileNotFoundError('COuld not get type of file')
            super(Recipe, self).__init__(self.recipe)  # make Dict from self
            # print list of local files *.img, *.hdr,*.pkl
            self._list_data()

        except FileNotFoundError as e:
            self.log.critical(e)
            raise SystemExit(-1)
        except json.decoder.JSONDecodeError as e:
            self.log.critical(e)
            raise SystemExit(-1)
        finally:
            if fo:
                fo.close()

    def get_channel(self, key: str):
        return self['channels'].get(key, [])

    def _list_data(self):
        try:
            all_files = os.listdir(self['DATADIR'])
            for f in all_files:
                ext = os.path.basename(f).split('.')[-1]
                if ext in ['img', 'hdr', 'pkl']:
                    self.log.debug(f"local file: {f}")
        except FileNotFoundError as e:
            self.log.warning(f'could not not list data-files: {e}')
        pass

    def cos_creds_content(self):
        if not os.path.isfile(self._cos_creds_file()):
            self.log.warning(f'COS credentials file not found "{self._cos_creds_file()}"')
            raise SystemExit(f'file "{self._cos_creds_file()}" not found')
        return json.load(open(self._cos_creds_file(), 'r'))

    def _cos_creds_file(self):
        return self.recipe['COS'].get('credentials', '/root/.bluemix/cos_credentials')

    def validate_schema(self):
        """
        Draft7 formats: https://json-schema.org/understanding-json-schema/reference/string.html
        for additional format validation refer https://python-jsonschema.readthedocs.io/en/stable/validate/
        for ex. enable uri checks:  sudo -H pip3 install   rfc3987
        :return:
        """
        try:
            import jsonsempai
            with jsonsempai.imports():
                from ocli.ai import recipe_schema
            # my_path = os.path.abspath(os.path.dirname(__file__))
            # path = os.path.join(my_path, "recipe_schema.json")
            # f = open(path, 'r')
            # schema = json.load(f)
            # f.close()
            schema = recipe_schema.properties.envelope
            v = Draft7Validator(schema, format_checker=draft7_format_checker)
            errors = sorted(v.iter_errors(self.recipe), key=lambda e: e.path)
            if not len(errors):
                self.log.info(f"recipe {self.file}: syntax is valid")
                return None
            for error in errors:
                self.log.error(f'{error.message} in {list(error.path)}')
                return 1
        except Exception as e:
            self.log.error(f'Could not perform validation: {e}')
            return 1
