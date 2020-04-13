import argparse
import os
from argparse import ArgumentParser

cmd_options = argparse.ArgumentParser(description='Generating COG image.')  # type: ArgumentParser

cmd_options.add_argument('--recipe',
                         type=str,
                         default=('./recipe.json'),
                         help='JSON recipe file with path, default is ./recipe.json')

cmd_options.add_argument('-v','--verbose',dest='verbose',
                         choices=('INFO', 'WARNING','ERROR','DEBUG'),
                         default=('INFO'),
                         help='path to task folder')

def check_recipe_exists(fname):
    if not os.path.isfile(fname):
        print("Error: Recipe file not found {} \n\n".format(fname))
        cmd_options.print_help()
        exit(-1)
