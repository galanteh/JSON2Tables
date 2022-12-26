#!/usr/bin/env python
# -*- coding: utf-8 -*-
# JSON2Tables

import argparse
import sys
import os

import pyarrow.json
from colorama import init, Fore, Style
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

__author__ = "Hernan Galante"
__copyright__ = "Copyright 2022, Hernan Galante"
__credits__ = ["Hernan Galante"]
__license__ = "GNU Software"
__version__ = "1.0"
__email__ = "hernan_galante@hotmail.com"
__status__ = "Production"


class JSON2TablesProgram:
    description_message = "Generate an ORC/Parquet table file from a bunch of flat jsons."
    exe_name = 'j2tables.exe'
    welcome_message = "JSON2Tables - Generate an ORC/Parquet table file from a bunch of flat jsons from a path."
    json_files = []
    file_type = ''

    def initialize_terminal_colors(self):
        """
        Initialize the terminal colors.
        """
        init(autoreset=True)
        print(Fore.YELLOW + Style.BRIGHT + self.welcome_message + Fore.WHITE + Style.RESET_ALL)

    def print_action(self, message):
        """
        Print into the console an action message.
        :param message:
        """
        print(Fore.GREEN + Style.BRIGHT + message + Fore.WHITE + Style.RESET_ALL)

    def print_error(self, message):
        """
        Print into the console an error message.
        :param message:
        """
        print(Fore.RED + Style.BRIGHT + message + Fore.WHITE + Style.RESET_ALL)
        sys.exit(-1)

    def validate_json_path(self, jsons_path):
        for root, dirs, files in os.walk(jsons_path):
            for file in files:
                if file.endswith('.json'):
                    self.json_files.append(os.path.join(root, file))
        return len(self.json_files) > 0

    def validate_output_file(self, output_file):
        dir_output = os.path.dirname(output_file)
        filename_output, file_ext_output = os.path.splitext(os.path.basename(output_file))
        if file_ext_output.lower().strip() in ['.orc']:
            self.file_type = 'ORC'
        else:
            self.file_type = 'PARQUET'
        return os.path.exists(dir_output)

    def validate_arguments(self, jsons_path, output_file):
        return self.validate_json_path(jsons_path) and self.validate_output_file(output_file)

    def _concat_tables(self, tables):
        new_table = None
        iter_range = range(len(tables)-1)
        for i in iter_range:
            if i == 0:
                new_table = tables[0]
            if i < len(tables):
                next_table = tables[i+1]
                tables_argument = [new_table, next_table]
                result_table = pa.concat_tables(tables_argument, True)
                new_table = result_table
        return new_table

    def _clean_up_null_values(self, table):
        pd_table = table.to_pandas()
        pd_table.fillna(value=0, inplace=True)
        return pa.table([]).from_pandas(pd_table)

    def _export_jsons_to_tables(self, output_file):
        tables = []
        for json_file in self.json_files:
            tables.append(pa.json.read_json(json_file))
        new_table = self._concat_tables(tables)
        new_table = self._clean_up_null_values(new_table)
        if self.file_type == 'ORC':
            self.print_action("Writing ORC file to: {}".format(output_file))
            orc.write_table(new_table, output_file, compression="zstd")
        else:
            self.print_action("Writing parquet file to: {}".format(output_file))
            writer = pq.ParquetWriter(output_file, new_table.schema, version="1.0", flavor="spark")
            writer.write(new_table)

    def run_json_2_tables(self, jsons_path, output_file):
        if self.validate_arguments(jsons_path, output_file):
            self._export_jsons_to_tables(output_file)
        else:
            self.print_error('Arguments are invalid. Please, check directories, extensions and permissions.')

    def run(self, argv):
        """
        Run the command line with argument to run a possible fixture
        """

        self.initialize_terminal_colors()
        parser = argparse.ArgumentParser(epilog=self.welcome_message,
                                         prog=self.exe_name,
                                         description=self.description_message)
        parser.add_argument('-i', '--jsons', nargs=1, default=None,
                            help="Path to the json files. Expected files with .json extension",
                            dest='INPUT', metavar='INPUT')
        parser.add_argument('-o', '--output', nargs=1, default=None,
                            help="Name of the ORC/Parquet file that will contain all the jsons information.",
                            dest='OUTPUT', metavar='OUTPUT')
        args = vars(parser.parse_args())

        try:
            if args['OUTPUT'] is None or args['INPUT'] is None:
                parser.print_help()
                sys.exit(1)
            if args['INPUT'][0] is not None:
                jsons_path = args['INPUT'][0]
            if args['OUTPUT'][0] is not None:
                output_file = args['OUTPUT'][0]
                self.validate_arguments(jsons_path, output_file)
                self.run_json_2_tables(jsons_path, output_file)
                self.print_action("Finish!")
                sys.exit(1)
        except Exception as e:
            self.print_error(str(e))
            parser.print_help()
        except KeyboardInterrupt as e:
            self.print_error("Process has been stopped")


if __name__ == "__main__":
    j2tbl_p = JSON2TablesProgram()
    j2tbl_p.run(sys.argv[1:])
