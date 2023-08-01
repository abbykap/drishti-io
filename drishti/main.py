#!/usr/bin/env python3

import os
import io
import sys
import csv
import time
import json
import shlex
import shutil
import datetime
import argparse
import subprocess
import matplotlib.pyplot as plt

import pandas as pd

import darshan
import darshan.backend.cffi_backend as darshanll

from rich import print, box, rule
from rich.console import Console, Group
from rich.padding import Padding
from rich.text import Text
from rich.syntax import Syntax
from rich.panel import Panel
from rich.terminal_theme import TerminalTheme
from rich.terminal_theme import MONOKAI
from subprocess import call

from packaging import version


RECOMMENDATIONS = 0
HIGH = 1
WARN = 2
INFO = 3
OK = 4

ROOT = os.path.abspath(os.path.dirname(__file__))

TARGET_USER = 1
TARGET_DEVELOPER = 2
TARGET_SYSTEM = 3

insights_operation = []
insights_metadata = []
insights_dxt = []

insights_total = dict()

insights_total[HIGH] = 0
insights_total[WARN] = 0
insights_total[RECOMMENDATIONS] = 0

THRESHOLD_OPERATION_IMBALANCE = 0.1
THRESHOLD_SMALL_REQUESTS = 0.1
THRESHOLD_SMALL_REQUESTS_ABSOLUTE = 1000
THRESHOLD_MISALIGNED_REQUESTS = 0.1
THRESHOLD_METADATA = 0.1
THRESHOLD_METADATA_TIME_RANK = 30  # seconds
THRESHOLD_RANDOM_OPERATIONS = 0.2
THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE = 1000
THRESHOLD_STRAGGLERS = 0.15
THRESHOLD_IMBALANCE = 0.30
THRESHOLD_INTERFACE_STDIO = 0.1
THRESHOLD_COLLECTIVE_OPERATIONS = 0.5
THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE = 1000

INSIGHTS_STDIO_HIGH_USAGE = 'S01'
INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE = 'P01'
INSIGHTS_POSIX_READ_COUNT_INTENSIVE = 'P02'
INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE = 'P03'
INSIGHTS_POSIX_READ_SIZE_INTENSIVE = 'P04'
INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE = 'P05'
INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE = 'P06'
INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE = 'P07'
INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE = 'P08'
INSIGHTS_POSIX_REDUNDANT_READ_USAGE = 'P09'
INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE = 'P10'
INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE = 'P11'
INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE = 'P12'
INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE = 'P13'
INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE = 'P14'
INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE = 'P15'
INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE = 'P16'
INSIGHTS_POSIX_HIGH_METADATA_TIME = 'P17'
INSIGHTS_POSIX_SIZE_IMBALANCE = 'P18'
INSIGHTS_POSIX_TIME_IMBALANCE = 'P19'
INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE = 'P21'
INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE = 'P22'
INSIGHTS_MPI_IO_NO_USAGE = 'M01'
INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE = 'M02'
INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE = 'M03'
INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE = 'M04'
INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE = 'M05'
INSIGHTS_MPI_IO_BLOCKING_READ_USAGE = 'M06'
INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE = 'M07'
INSIGHTS_MPI_IO_AGGREGATORS_INTRA = 'M08'
INSIGHTS_MPI_IO_AGGREGATORS_INTER = 'M09'
INSIGHTS_MPI_IO_AGGREGATORS_OK = 'M10'

# TODO: need to verify the threashold to be between 0 and 1
# TODO: read thresholds from file

parser = argparse.ArgumentParser(
    description='Drishti: '
)

parser.add_argument(
    'darshan',
    help='Input .darshan file'
)

parser.add_argument(
    '--issues',
    default=False,
    action='store_true',
    dest='only_issues',
    help='Only displays the detected issues and hides the recommendations'
)

parser.add_argument(
    '--html',
    default=False,
    action='store_true',
    dest='export_html',
    help='Export the report as an HTML page'
)

parser.add_argument(
    '--svg',
    default=False,
    action='store_true',
    dest='export_svg',
    help='Export the report as an SVG image'
)

parser.add_argument(
    '--light',
    default=False,
    action='store_true',
    dest='export_theme_light',
    help='Use a light theme for the report when generating files'
)

parser.add_argument(
    '--size',
    default=False,
    dest='export_size',
    help='Console width used for the report and generated files'
)

parser.add_argument(
    '--verbose',
    default=False,
    action='store_true',
    dest='verbose',
    help='Display extended details for the recommendations'
)

parser.add_argument(
    '--code',
    default=False,
    action='store_true',
    dest='code',
    help='Display insights identification code'
)

parser.add_argument(
    '--path',
    default=False,
    action='store_true',
    dest='full_path',
    help='Display the full file path for the files that triggered the issue'
)

parser.add_argument(
    '--csv',
    default=False,
    action='store_true',
    dest='export_csv',
    help='Export a CSV with the code of all issues that were triggered'
)

parser.add_argument(
    '--json', 
    default=False, 
    dest='json',
    help=argparse.SUPPRESS)

args = parser.parse_args()

if args.export_size:
    console = Console(record=True, width=int(args.export_size))
else:
    console = Console(record=True)

csv_report = []


def validate_thresholds():
    """
    Validate thresholds defined by the user.
    """
    assert(THRESHOLD_OPERATION_IMBALANCE >= 0.0 and THRESHOLD_OPERATION_IMBALANCE <= 1.0)
    assert(THRESHOLD_SMALL_REQUESTS >= 0.0 and THRESHOLD_SMALL_REQUESTS <= 1.0)
    assert(THRESHOLD_MISALIGNED_REQUESTS >= 0.0 and THRESHOLD_MISALIGNED_REQUESTS <= 1.0)
    assert(THRESHOLD_METADATA >= 0.0 and THRESHOLD_METADATA <= 1.0)
    assert(THRESHOLD_RANDOM_OPERATIONS >= 0.0 and THRESHOLD_RANDOM_OPERATIONS <= 1.0)

    assert(THRESHOLD_METADATA_TIME_RANK >= 0.0)


def clear():
    """
    Clear the screen with the comment call based on the operating system.
    """
    _ = call('clear' if os.name == 'posix' else 'cls')


def convert_bytes(bytes_number):
    """
    Convert bytes into formatted string.
    """
    tags = [
        'bytes',
        'KB',
        'MB',
        'GB',
        'TB',
        'PB',
        'EB'
    ]

    i = 0
    double_bytes = bytes_number

    while (i < len(tags) and  bytes_number >= 1024):
        double_bytes = bytes_number / 1024.0
        i = i + 1
        bytes_number = bytes_number / 1024

    return str(round(double_bytes, 2)) + ' ' + tags[i] 


def is_available(name):
    """Check whether `name` is on PATH and marked as executable."""

    return shutil.which(name) is not None


def message(code, target, level, issue, recommendations=None, details=None):
    """
    Display the message on the screen with level, issue, and recommendation.
    """
    icon = ':arrow_forward:'

    if level in (HIGH, WARN):
        insights_total[level] += 1

    if level == HIGH:
        color = '[red]'
    elif level == WARN:
        color = '[orange1]'
    elif level == OK:
        color = '[green]'
    else:
        color = ''

    messages = [
        '{}{}{} {}'.format(
            color,
            icon,
            ' [' + code + ']' if args.code else '',
            issue
        )
    ]

    if args.export_csv:
        csv_report.append(code)

    if details:
        for detail in details:
            messages.append('  {}:left_arrow_curving_right: {}'.format(
                    color,
                    detail['message']
                )
            )

    if recommendations:
        if not args.only_issues:
            messages.append('  [white]:left_arrow_curving_right: [b]Recommendations:[/b]')

            for recommendation in recommendations:
                messages.append('    :left_arrow_curving_right: {}'.format(recommendation['message']))

                if args.verbose and 'sample' in recommendation:
                    messages.append(
                        Padding(
                            Panel(
                                recommendation['sample'],
                                title='Solution Example Snippet',
                                title_align='left',
                                padding=(1, 2)
                            ),
                            (1, 0, 1, 7)
                        )
                    )

        insights_total[RECOMMENDATIONS] += len(recommendations)

    return Group(
        *messages
    )


def check_log_version(file, log_version, library_version):
    use_file = file

    if version.parse(log_version) < version.parse('3.4.0'):
        # Check if darshan-convert is installed and available in the PATH
        if not is_available('darshan-convert'):
            console.print(
                Panel(
                    Padding(
                        'Darshan file is using an old format and darshan-convert is not available in the PATH.',
                        (1, 1)
                    ),
                    title='{}WARNING'.format('[orange1]'),
                    title_align='left'
                )
            )

            sys.exit(os.EX_DATAERR)

        use_file = os.path.basename(file.replace('.darshan', '.converted.darshan'))

        console.print(
            Panel(
                Padding(
                    'Converting .darshan log from {} to 3.4.0: format: saving output file "{}" in the current working directory.'.format(
                        log_version,
                        use_file
                    ),
                    (1, 1)
                ),
                title='{}WARNING'.format('[orange1]'),
                title_align='left'
            )
        )

        if not os.path.isfile(use_file):
            ret = os.system(
                'darshan-convert {} {}'.format(
                    file,
                    use_file
                )
            )

            if ret != 0:
                print('Unable to convert .darshan file to version {}'.format(library_version))

    return use_file


def main():
    if not os.path.isfile(args.darshan):
        print('Unable to open .darshan file.')

        sys.exit(os.EX_NOINPUT)

    # clear()
    validate_thresholds()

    insights_start_time = time.time()

    log = darshanll.log_open(args.darshan)

    modules = darshanll.log_get_modules(log)

    information = darshanll.log_get_job(log)

    log_version = information['metadata']['lib_ver']
    library_version = darshanll.darshan.backend.cffi_backend.get_lib_version()

    # Make sure log format is of the same version
    filename = check_log_version(args.darshan, log_version, library_version)
 
    darshanll.log_close(log)

    darshan.enable_experimental()

    report = darshan.DarshanReport(filename)

    job = report.metadata

    #########################################################################################################################################################################

    # Check usage of STDIO, POSIX, and MPI-IO per file

    if 'STDIO' in report.records:
        df_stdio = report.records['STDIO'].to_df()

        if df_stdio:
            total_write_size_stdio = df_stdio['counters']['STDIO_BYTES_WRITTEN'].sum()
            total_read_size_stdio = df_stdio['counters']['STDIO_BYTES_READ'].sum()

            total_size_stdio = total_write_size_stdio + total_read_size_stdio 
        else:
            total_size_stdio = 0
    else:
        df_stdio = None

        total_size_stdio = 0

    if 'POSIX' in report.records:
        df_posix = report.records['POSIX'].to_df()

        if df_posix:
            total_write_size_posix = df_posix['counters']['POSIX_BYTES_WRITTEN'].sum()
            total_read_size_posix = df_posix['counters']['POSIX_BYTES_READ'].sum()

            total_size_posix = total_write_size_posix + total_read_size_posix
        else:
            total_size_posix = 0
    else:
        df_posix = None

        total_size_posix = 0

    if 'MPI-IO' in report.records:
        df_mpiio = report.records['MPI-IO'].to_df()

        if df_mpiio:
            total_write_size_mpiio = df_mpiio['counters']['MPIIO_BYTES_WRITTEN'].sum()
            total_read_size_mpiio = df_mpiio['counters']['MPIIO_BYTES_READ'].sum()

            total_size_mpiio = total_write_size_mpiio + total_read_size_mpiio 
        else:
            total_size_mpiio = 0
    else:
        df_mpiio = None

        total_size_mpiio = 0

    # Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
    if total_size_posix > 0 and total_size_posix >= total_size_mpiio:
        total_size_posix -= total_size_mpiio

    total_size = total_size_stdio + total_size_posix + total_size_mpiio

    assert(total_size_stdio >= 0)
    assert(total_size_posix >= 0)
    assert(total_size_mpiio >= 0)

    files = {}

    # Check interface usage for each file
    file_map = report.name_records

    total_files = len(file_map)

    total_files_stdio = 0
    total_files_posix = 0
    total_files_mpiio = 0

    for id, path in file_map.items():
        if df_stdio:
            uses_stdio = len(df_stdio['counters'][(df_stdio['counters']['id'] == id)]) > 0
        else:
            uses_stdio = 0
        
        if df_posix:
            uses_posix = len(df_posix['counters'][(df_posix['counters']['id'] == id)]) > 0
        else:
            uses_posix = 0

        if df_mpiio:
            uses_mpiio = len(df_mpiio['counters'][(df_mpiio['counters']['id'] == id)]) > 0
        else:
            uses_mpiio = 0

        total_files_stdio += uses_stdio
        total_files_posix += uses_posix
        total_files_mpiio += uses_mpiio

        files[id] = {
            'path': path,
            'stdio': uses_stdio,
            'posix': uses_posix,
            'mpiio': uses_mpiio
        }

    df_posix_files = df_posix

    if total_size and total_size_stdio / total_size > THRESHOLD_INTERFACE_STDIO:
        issue = 'Application is using STDIO, a low-performance interface, for {:.2f}% of its data transfers ({})'.format(
            total_size_stdio / total_size * 100.0,
            convert_bytes(total_size_stdio)
        )
       
        recommendation = [
            {
                'message': 'Consider switching to a high-performance I/O interface such as MPI-IO'
            }
        ]

        insights_operation.append(
            message(INSIGHTS_STDIO_HIGH_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    if 'MPI-IO' not in modules:
        issue = 'Application is using low-performance interface'

        recommendation = [
            {
                'message' : 'Consider switching to a high-performance I/O interface such as MPI-IO'
            }
        ]

        insights_operation.append(
            message(INSIGHTS_MPI_IO_NO_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
        )

    #########################################################################################################################################################################

    if 'POSIX' in report.records:
        df = report.records['POSIX'].to_df()

        #print(df)
        #print(df['counters'].columns)
        #print(df['fcounters'].columns)

        #########################################################################################################################################################################

        # Get number of write/read operations
        total_reads = df['counters']['POSIX_READS'].sum()
        total_writes = df['counters']['POSIX_WRITES'].sum()

        # Get total number of I/O operations
        total_operations = total_writes + total_reads 

        # To check whether the application is write-intersive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
        if total_writes > total_reads and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is write operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
                total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
            )

            insights_metadata.append(
                message(INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        if total_reads > total_writes and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is read operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
                total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
            )

            insights_metadata.append(
                message(INSIGHTS_POSIX_READ_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        total_read_size = df['counters']['POSIX_BYTES_READ'].sum()
        total_written_size = df['counters']['POSIX_BYTES_WRITTEN'].sum()

        total_size = total_written_size + total_read_size

        if total_written_size > total_read_size and abs(total_written_size - total_read_size) / (total_written_size + total_read_size) > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is write size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
                total_written_size / (total_written_size + total_read_size) * 100.0, total_read_size / (total_written_size + total_read_size) * 100.0
            )

            insights_metadata.append(
                message(INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        if total_read_size > total_written_size and abs(total_written_size - total_read_size) / (total_written_size + total_read_size) > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is read size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
                total_written_size / (total_written_size + total_read_size) * 100.0, total_read_size / (total_written_size + total_read_size) * 100.0
            )

            insights_metadata.append(
                message(INSIGHTS_POSIX_READ_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        #########################################################################################################################################################################
        
        # Get the number of small I/O operations (less than 1 MB)

        # Get the files responsible for more than half of these accesses
        files = []

        df['counters']['INSIGHTS_POSIX_SMALL_READ'] = (
            df['counters']['POSIX_SIZE_READ_0_100'] +
            df['counters']['POSIX_SIZE_READ_100_1K'] +
            df['counters']['POSIX_SIZE_READ_1K_10K'] +
            df['counters']['POSIX_SIZE_READ_10K_100K'] +
            df['counters']['POSIX_SIZE_READ_100K_1M']
        )

        df['counters']['INSIGHTS_POSIX_SMALL_WRITE'] = (
            df['counters']['POSIX_SIZE_WRITE_0_100'] +
            df['counters']['POSIX_SIZE_WRITE_100_1K'] +
            df['counters']['POSIX_SIZE_WRITE_1K_10K'] +
            df['counters']['POSIX_SIZE_WRITE_10K_100K'] +
            df['counters']['POSIX_SIZE_WRITE_100K_1M']
        )
        total_reads_small = (
                df['counters']['POSIX_SIZE_READ_0_100'].sum() +
                df['counters']['POSIX_SIZE_READ_100_1K'].sum() +
                df['counters']['POSIX_SIZE_READ_1K_10K'].sum() +
                df['counters']['POSIX_SIZE_READ_10K_100K'].sum() +
                df['counters']['POSIX_SIZE_READ_100K_1M'].sum()
            )

        posix_size_read_0_100 = df['counters']['POSIX_SIZE_READ_0_100'].sum()
        posix_size_read_100_1K = df['counters']['POSIX_SIZE_READ_100_1K'].sum()
        posix_size_read_1K_10K = df['counters']['POSIX_SIZE_READ_1K_10K'].sum()
        posix_size_read_10K_100K = df['counters']['POSIX_SIZE_READ_10K_100K'].sum()
        posix_size_read_100K_1M = df['counters']['POSIX_SIZE_READ_100K_1M'].sum()
        read_larger_than_1MB = total_reads - total_reads_small

        data = [posix_size_read_0_100, posix_size_read_100_1K, posix_size_read_1K_10K, posix_size_read_10K_100K, posix_size_read_100K_1M, read_larger_than_1MB]  # Numeric data for each category
        categories = ['0-100', '100-1K', '1K-10K', '100K-1M', '100K-1M', 'Everything else']  # Category labels

        plt.pie(data, labels=categories, autopct='%1.1f%%')

        plt.title('Small Read Size Intensive')

        graph = plt.savefig('graph1.png')

        detected_files = pd.DataFrame(df['counters'].groupby('id')[['INSIGHTS_POSIX_SMALL_READ', 'INSIGHTS_POSIX_SMALL_WRITE']].sum()).reset_index()
        detected_files.columns = ['id', 'total_reads', 'total_writes']
        detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)

        if total_reads_small and total_reads_small / total_reads > THRESHOLD_SMALL_REQUESTS and total_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'AppDIEEEEEElication issues a high number ({}) of small read requests (i.e., < 1MB) which represents {:.2f}% of all read requests'.format(
                total_reads_small, total_reads_small / total_reads * 100.0
            )

            detail = []
            recommendation = []

            for index, row in detected_files.iterrows():
                if row['total_reads'] > (total_reads * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                                row['total_reads'],
                                row['total_reads'] / total_reads * 100.0,
                                file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                            ) 
                        }
                    )

            recommendation.append(
                {
                    'message': 'Consider buffering read operations into larger more contiguous ones',
                    'graph' : "graph1.png"
                }
            )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since the appplication already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_read_all() or MPI_File_read_at_all()) to aggregate requests into larger ones',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default'),
                        'graph' : "graph1.png"

                    }
                )
            else:
                recommendation.append(
                    {
                        'message': 'Application does not use MPI-IO for operations, consider use this interface instead to harness collective operations',
                        'graph' : "graph1.png"

                    }
                )
            for rec in recommendation:
                graph_path = rec.get('graph')
                if graph_path:
                    try:
                        subprocess.run(['imgcat', graph_path])
                    except FileNotFoundError:
                        print(f"Warning: 'imgcat' command not found. Please install 'imgcat' to display the graphs in the terminal.")
                    except Exception as e:
                        print(f"Error displaying graph: {e}")
                    
            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail, graph)
            )


            total_writes_small = (
                    df['counters']['POSIX_SIZE_WRITE_0_100'].sum() +
                    df['counters']['POSIX_SIZE_WRITE_100_1K'].sum() +
                    df['counters']['POSIX_SIZE_WRITE_1K_10K'].sum() +
                    df['counters']['POSIX_SIZE_WRITE_10K_100K'].sum() +
                    df['counters']['POSIX_SIZE_WRITE_100K_1M'].sum()
                )

            posix_size_write_0_100 = df['counters']['POSIX_SIZE_WRITE_0_100'].sum()
            posix_size_write_100_1K = df['counters']['POSIX_SIZE_WRITE_100_1K'].sum()
            posix_size_write_1K_10K = df['counters']['POSIX_SIZE_WRITE_1K_10K'].sum()
            posix_size_write_10K_100K = df['counters']['POSIX_SIZE_WRITE_10K_100K'].sum()
            posix_size_write_100K_1M = df['counters']['POSIX_SIZE_WRITE_100K_1M'].sum()
            write_larger_than_1MB = total_writes - total_writes_small

            #Sample data
            data = [posix_size_write_0_100, posix_size_write_100_1K, posix_size_write_1K_10K, posix_size_write_10K_100K, posix_size_write_100K_1M, write_larger_than_1MB]  # Numeric data for each category
            categories = ['0-100', '100-1K', '1K-10K', '100K-1M', '100K-1M', 'Everything else']  # Category labels

            # Create a pie chart
            plt.pie(data, labels=categories, autopct='%1.1f%%')

            # Add a title
            plt.title('Small Read Size Intensive')

            # Display the chart
            plt.savefig('graph2.png')
        # Get the number of small I/O operations (less than the stripe size)

        if total_writes_small and total_writes_small / total_writes > THRESHOLD_SMALL_REQUESTS and total_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'Application issues a high number ({}) of small write requests (i.e., < 1MB) which represents {:.2f}% of all write requests'.format(
                total_writes_small, total_writes_small / total_writes * 100.0
            )

            detail = []
            recommendation = []

            for index, row in detected_files.iterrows():
                if row['total_writes'] > (total_writes * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small write requests are to "{}"'.format(
                                row['total_writes'],
                                row['total_writes'] / total_writes * 100.0,
                                file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                            ) 
                        }
                    )

            recommendation.append(
                {
                    'message': 'Consider buffering write operations into larger more contiguous ones',
                    'graph' : 'graph2.png'
                }
            )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since the application already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_write_all() or MPI_File_write_at_all()) to aggregate requests into larger ones',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph2.png'

                    }
                )
            else:
                recommendation.append(
                    {
                        'message': 'Application does not use MPI-IO for operations, consider use this interface instead to harness collective operations',
                        'graph' : 'graph2.png'

                    }
                )
            for item in recommendation:
                graph_path = item.get('graph')
                if graph_path:
                    subprocess.run(['xdg=open', graph_path])

            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        #########################################################################################################################################################################

        # How many requests are misaligned?

        jobs = df['counters']['jobid'].tolist()
        misaligned_requests = df['counters']['POSIX_FILE_NOT_ALIGNED'].tolist()
        # Assuming you also have the misaligned_requests data (similar to the 'misaligned_requests' list in your example)

        # Plot the misaligned POSIX file requests for different jobs
        plt.figure(figsize=(10, 6))
        plt.bar(jobs, misaligned_requests, color='b')
        plt.xlabel('Jobs')
        plt.ylabel('Misaligned POSIX File Requests')
        plt.title('Misaligned POSIX File Requests for Different Jobs')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('graph3.png')
        total_mem_not_aligned = df['counters']['POSIX_MEM_NOT_ALIGNED'].sum()
        total_file_not_aligned = df['counters']['POSIX_FILE_NOT_ALIGNED'].sum()

        if total_operations and total_mem_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
            issue = 'Application has a high number ({:.2f}%) of misaligned memory requests'.format(
                total_mem_not_aligned / total_operations * 100.0
            )

            insights_metadata.append(
                message(INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE, TARGET_DEVELOPER, HIGH, issue, None)
            )

        if total_operations and total_file_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
            issue = 'Application issues a high number ({:.2f}%) of misaligned file requests'.format(
                total_file_not_aligned / total_operations * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider aligning the requests to the file system block boundaries',
                    'graph' : 'graph3.png'
                }
            ]

            if 'HF5' in modules:
                recommendation.append(
                    {
                        'message': 'Since the appplication uses HDF5, consider using H5Pset_alignment() in a file access property list',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-alignment.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph3.png'

                    },
                    {
                        'message': 'Any file object greater than or equal in size to threshold bytes will be aligned on an address which is a multiple of alignment',
                        'graph' : 'graph3.png'

                    }
                )

            if 'LUSTRE' in modules:
                recommendation.append(
                    {
                        'message': 'Consider using a Lustre alignment that matches the file system stripe configuration',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default'),
                        'graph' : 'graph3.png'

                    }
                )
            for item in recommendation:
                graph_path = item.get('graph')
                if graph_path:
                    subprocess.run(['xdg=open', graph_path])

            insights_metadata.append(
                message(INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
            )

        #########################################################################################################################################################################

        max_read_offset = df['counters']['POSIX_MAX_BYTE_READ'].tolist()
        bytes_read = df['counters']['POSIX_BYTES_READ'].tolist()
        max_write_offset = df['counters']['POSIX_MAX_BYTE_WRITE'].tolist()
        bytes_written = bytes_read = df['counters']['POSIX_BYTES_WRITTEN'].tolist()

        plt.figure(figsize=(12, 10))

        # Scatter Plot for Read Operations
        plt.subplot(2, 2, 1)
        for i in range(len(jobs)):
            plt.scatter(max_read_offset[i], bytes_read[i], marker='o', label=jobs[i])
        plt.xlabel('Highest Read Offset (POSIX_MAX_BYTE_READ)')
        plt.ylabel('Bytes Read (POSIX_BYTES_READ)')
        plt.title('Highest Read Offset vs. Bytes Read')
        plt.legend()

        # Scatter Plot for Write Operations
        plt.subplot(2, 2, 2)
        for i in range(len(jobs)):
            plt.scatter(max_write_offset[i], bytes_written[i], marker='o', label=jobs[i])
        plt.xlabel('Highest Write Offset (POSIX_MAX_BYTE_WRITTEN)')
        plt.ylabel('Bytes Written (POSIX_BYTES_WRITTEN)')
        plt.title('Highest Write Offset vs. Bytes Written')
        plt.legend()

        # Histogram for Redundant Read Ratio
        redundant_read_ratio = [bytes_read[i] / max_read_offset[i] for i in range(len(jobs))]
        plt.subplot(2, 2, 3)
        plt.hist(redundant_read_ratio, bins=10, color='blue', alpha=0.7)
        plt.xlabel('Redundant Read Ratio')
        plt.ylabel('Frequency')
        plt.title('Distribution of Redundant Read Ratio')

        # Histogram for Redundant Write Ratio
        redundant_write_ratio = [bytes_written[i] / max_write_offset[i] for i in range(len(jobs))]
        plt.subplot(2, 2, 4)
        plt.hist(redundant_write_ratio, bins=10, color='red', alpha=0.7)
        plt.xlabel('Redundant Write Ratio')
        plt.ylabel('Frequency')
        plt.title('Distribution of Redundant Write Ratio')

        plt.tight_layout()
        plt.savefig('graphredundant.png')
        # Redundant read-traffic (based on Phill)
        # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
        max_read_offset = df['counters']['POSIX_MAX_BYTE_READ'].max()

        if max_read_offset > total_read_size:
            issue = 'Application might have redundant read traffic (more data read than the highest offset)'

            insights_metadata.append(
            {
                'message': message(INSIGHTS_POSIX_REDUNDANT_READ_USAGE, TARGET_DEVELOPER, WARN, issue, None),
                'graph': 'graphredundant.png'
            }
        )


        max_write_offset = df['counters']['POSIX_MAX_BYTE_WRITTEN'].max()
        for item in recommendation:
                graph_path = item.get('graph')
                if graph_path:
                    subprocess.run(['xdg=open', graph_path])
        if max_write_offset > total_written_size:
            issue = 'Application might have redundant write traffic (more data written than the highest offset)'

            insights_metadata.append(
            {
                'message' : message(INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, None),
                'graph' : 'graphredundant.png'
            }
        )
        for rec in recommendation:
            graph_path = rec.get('graph')
            if graph_path:
                try:
                    subprocess.run(['xdg-open', graph_path])
                except FileNotFoundError:
                    print(f"Warning: 'xdg-open' command not found. Please make sure 'xdg-utils' is installed to open the graph.")
                except Exception as e:
                    print(f"Error opening graph: {e}")
        #########################################################################################################################################################################

 
        read_consecutive = df['counters']['POSIX_CONSEC_READS'].sum()
        #print('READ Consecutive: {} ({:.2f}%)'.format(read_consecutive, read_consecutive / total_reads * 100))

        read_sequential = df['counters']['POSIX_SEQ_READS'].sum()
        read_sequential -= read_consecutive
        #print('READ Sequential: {} ({:.2f}%)'.format(read_sequential, read_sequential / total_reads * 100))

        read_random = total_reads - read_consecutive - read_sequential
        #print('READ Random: {} ({:.2f}%)'.format(read_random, read_random / total_reads * 100))
        total_reads = read_consecutive + read_sequential + read_random

        # Calculate percentages
        percent_consecutive = read_consecutive / total_reads * 100
        percent_sequential = read_sequential / total_reads * 100
        percent_random = read_random / total_reads * 100

        # Plot the breakdown of read operations into consecutive, sequential, and random
        plt.figure(figsize=(8, 6))
        plt.bar("Read Operations", percent_random, color='red', label='Random')
        plt.bar("Read Operations", percent_sequential, bottom=percent_random, color='orange', label='Sequential')
        plt.bar("Read Operations", percent_consecutive, bottom=percent_random + percent_sequential, color='green', label='Consecutive')

        plt.xlabel('Operations')
        plt.ylabel('Percentage of Total Reads')
        plt.title('Breakdown of Read Operations')
        plt.legend(loc='upper right')

        plt.ylim(0, 100)  # Set the y-axis limit from 0 to 100 for percentage representation
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('graph4.png')

        if total_reads:
            if read_random and read_random / total_reads > THRESHOLD_RANDOM_OPERATIONS and read_random > THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE:
                issue = 'Application is issuing a high number ({}) of random read operations ({:.2f}%)'.format(
                    read_random, read_random / total_reads * 100.0
                )

                recommendation = [
                    {
                        'message': 'Consider changing your data model to have consecutive or sequential reads',
                        'graph' : 'graph4.png'
                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
                )
            else:
                issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) read requests'.format(
                    read_consecutive / total_reads * 100.0,
                    read_sequential / total_reads * 100.0
                )

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE, TARGET_DEVELOPER, OK, issue, None)
                )

        write_consecutive = df['counters']['POSIX_CONSEC_WRITES'].sum()
        #print('WRITE Consecutive: {} ({:.2f}%)'.format(write_consecutive, write_consecutive / total_writes * 100))

        write_sequential = df['counters']['POSIX_SEQ_WRITES'].sum()
        write_sequential -= write_consecutive
        #print('WRITE Sequential: {} ({:.2f}%)'.format(write_sequential, write_sequential / total_writes * 100))

        write_random = total_writes - write_consecutive - write_sequential
        #print('WRITE Random: {} ({:.2f}%)'.format(write_random, write_random / total_writes * 100))

        if total_writes:
            if write_random and write_random / total_writes > THRESHOLD_RANDOM_OPERATIONS and write_random > THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE:
                issue = 'Application is issuing a high number ({}) of random write operations ({:.2f}%)'.format(
                    write_random, write_random / total_writes * 100.0
                )

                recommendation = [
                    {
                        'message': 'Consider changing your data model to have consecutive or sequential writes',
                        'graph' : 'graph4.png'

                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
                )
            else:
                issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) write requests'.format(
                    write_consecutive / total_writes * 100.0,
                    write_sequential / total_writes * 100.0
                )

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE, TARGET_DEVELOPER, OK, issue, None)
                )

        #########################################################################################################################################################################

    
            # Shared file with small operations
            # print(df['counters'].loc[(df['counters']['rank'] == -1)])

            shared_files = df['counters'].loc[(df['counters']['rank'] == -1)]

            shared_files = shared_files.assign(id=lambda d: d['id'].astype(str))

            if not shared_files.empty:
                total_shared_reads = shared_files['POSIX_READS'].sum()
                total_shared_reads_small = (
                    shared_files['POSIX_SIZE_READ_0_100'].sum() +
                    shared_files['POSIX_SIZE_READ_100_1K'].sum() +
                    shared_files['POSIX_SIZE_READ_1K_10K'].sum() +
                    shared_files['POSIX_SIZE_READ_10K_100K'].sum() +
                    shared_files['POSIX_SIZE_READ_100K_1M'].sum()
                )

                shared_files['INSIGHTS_POSIX_SMALL_READS'] = (
                    shared_files['POSIX_SIZE_READ_0_100'] +
                    shared_files['POSIX_SIZE_READ_100_1K'] +
                    shared_files['POSIX_SIZE_READ_1K_10K'] +
                    shared_files['POSIX_SIZE_READ_10K_100K'] +
                    shared_files['POSIX_SIZE_READ_100K_1M']
                )

    
            # Create the KDE plot
            plt.figure(figsize=(8, 6))
            sns.kdeplot(data=total_shared_reads_small, color='blue', shade=True)

            # Calculate x and y values for markers
            x_values = [sum(total_shared_reads_small) / len(total_shared_reads_small)] * len(total_shared_reads_small)
            y_values = [0] * len(total_shared_reads_small)

            # Overlay a histogram on top of the KDE plot
            sns.histplot(data=total_shared_reads_small, color='lightblue', bins=10, kde=False)

            # Plot x and y value markers
            for x, y, value in zip(x_values, y_values, total_shared_reads_small):
                plt.text(x, y, str(value), ha='center', va='bottom', fontsize=10)

            # Add x and y labels
            plt.xlabel('Total Shared Reads (Small)')
            plt.ylabel('Density')
            plt.title('Kernel Density Estimation of Total Shared Reads (Small)')

            plt.tight_layout()
            plt.savefig('graph5.png')



            if total_shared_reads and total_shared_reads_small / total_shared_reads > THRESHOLD_SMALL_REQUESTS and total_shared_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
                issue = 'Application issues a high number ({}) of small read requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file read requests'.format(
                    total_shared_reads_small, total_shared_reads_small / total_shared_reads * 100.0
                )

                detail = []

                for index, row in shared_files.iterrows():
                    if row['INSIGHTS_POSIX_SMALL_READS'] > (total_shared_reads * THRESHOLD_SMALL_REQUESTS / 2):
                        detail.append(
                            {
                                'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                                    row['INSIGHTS_POSIX_SMALL_READS'],
                                    row['INSIGHTS_POSIX_SMALL_READS'] / total_shared_reads * 100.0,
                                    file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                ) 
                            }
                        )

                recommendation = [
                    {
                        'message': 'Consider coalesceing read requests into larger more contiguous ones using MPI-IO collective operations',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph5.png'
                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )

            total_shared_writes = shared_files['POSIX_WRITES'].sum()
            total_shared_writes_small = (
                shared_files['POSIX_SIZE_WRITE_0_100'].sum() +
                shared_files['POSIX_SIZE_WRITE_100_1K'].sum() +
                shared_files['POSIX_SIZE_WRITE_1K_10K'].sum() +
                shared_files['POSIX_SIZE_WRITE_10K_100K'].sum() +
                shared_files['POSIX_SIZE_WRITE_100K_1M'].sum()
            )

            shared_files['INSIGHTS_POSIX_SMALL_WRITES'] = (
                shared_files['POSIX_SIZE_WRITE_0_100'] +
                shared_files['POSIX_SIZE_WRITE_100_1K'] +
                shared_files['POSIX_SIZE_WRITE_1K_10K'] +
                shared_files['POSIX_SIZE_WRITE_10K_100K'] +
                shared_files['POSIX_SIZE_WRITE_100K_1M']
            )

            # Create the KDE plot
            plt.figure(figsize=(8, 6))
            sns.kdeplot(data=total_shared_writes_small, color='blue', shade=True)

            # Calculate x and y values for markers
            x_values = [sum(total_shared_writes_small) / len(total_shared_writes_small)] * len(total_shared_writes_small)
            y_values = [0] * len(total_shared_writes_small)

            # Overlay a histogram on top of the KDE plot
            sns.histplot(data=total_shared_writes_small, color='lightblue', bins=10, kde=False)

            # Plot x and y value markers
            for x, y, value in zip(x_values, y_values, total_shared_reads_small):
                plt.text(x, y, str(value), ha='center', va='bottom', fontsize=10)

            # Add x and y labels
            plt.xlabel('Total Shared Writes (Small)')
            plt.ylabel('Density')
            plt.title('Kernel Density Estimation of Total Shared Writes (Small)')

            plt.tight_layout()
            plt.savefig('graph55.png')
            if total_shared_writes and total_shared_writes_small / total_shared_writes > THRESHOLD_SMALL_REQUESTS and total_shared_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
                issue = 'Application issues a high number ({}) of small write requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file write requests'.format(
                    total_shared_writes_small, total_shared_writes_small / total_shared_writes * 100.0
                )

                detail = []

                for index, row in shared_files.iterrows():
                    if row['INSIGHTS_POSIX_SMALL_WRITES'] > (total_shared_writes * THRESHOLD_SMALL_REQUESTS / 2):
                        detail.append(
                            {
                                'message': '{} ({:.2f}%) small writes requests are to "{}"'.format(
                                    row['INSIGHTS_POSIX_SMALL_WRITES'],
                                    row['INSIGHTS_POSIX_SMALL_WRITES'] / total_shared_writes * 100.0,
                                    file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                ),
                                'graph' : 'graph55.png'
                            }
                        )

                recommendation = [
                    {
                        'message': 'Consider coalescing write requests into larger more contiguous ones using MPI-IO collective operations',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph55.png'

                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )

        #########################################################################################################################################################################

        
        has_long_metadata = df['fcounters'][(df['fcounters']['POSIX_F_META_TIME'] > THRESHOLD_METADATA_TIME_RANK)]


        # Create the grouped bar chart
        metrics = ['Number of Ranks with Long Metadata']
        values = [has_long_metadata]

        plt.figure(figsize=(6, 6))
        plt.bar(metrics, values, color='b')
        plt.xlabel('Metrics')
        plt.ylabel('Counts')
        plt.title('Number of Ranks with Long Metadata Operations')
        plt.ylim(0, max(values) * 1.2)  # Set the y-axis limit with some buffer space

        # Add annotations with specific Darshan counter information
        plt.annotate('Threshold: {} seconds'.format(THRESHOLD_METADATA_TIME_RANK), xy=(0, has_long_metadata), xytext=(0.5, has_long_metadata_ranks + 0.2),
                    arrowprops=dict(arrowstyle='->'), ha='center')

        plt.tight_layout()
        plt.savefig('graph6.png')
        has_long_metadata = df['fcounters'][(df['fcounters']['POSIX_F_META_TIME'] > THRESHOLD_METADATA_TIME_RANK)]

        if not has_long_metadata.empty:
            issue = 'There are {} ranks where metadata operations take over {} seconds'.format(
                len(has_long_metadata), THRESHOLD_METADATA_TIME_RANK
            )

            recommendation = [
                {
                    'message': 'Attempt to combine files, reduce, or cache metadata operations',
                    'graph' : 'graph6.png'
                }
            ]

            if 'HF5' in modules:
                recommendation.append(
                    {
                        'message': 'Since your appplication uses HDF5, try enabling collective metadata calls with H5Pset_coll_metadata_write() and H5Pset_all_coll_metadata_ops()',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-collective-metadata.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph6.png'

                    },
                    {
                        'message': 'Since your appplication uses HDF5, try using metadata cache to defer metadata operations',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-cache.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph6.png'

                    }
                )

            insights_metadata.append(
                message(INSIGHTS_POSIX_HIGH_METADATA_TIME, TARGET_DEVELOPER, HIGH, issue, recommendation)
            )

        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the 

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_F_VARIANCE_RANK_BYTES
# Hypothetical data (replace with actual Darshan counter data)
        shared_files = pd.DataFrame({
            'rank': df['counters']['rank'].tolist(),
            'POSIX_BYTES_WRITTEN': df['counters']['POSIX_BYTES_WRITTEN'].tolist(),
            'POSIX_BYTES_READ': df['counters']['POSIX_BYTES_READ'].tolist(),
            'POSIX_FASTEST_RANK_BYTES': df['counters']['POSIX_FASTEST_RANK_BYTES'].tolist(),
            'POSIX_SLOWEST_RANK_BYTES': df['counters']['POSIX_SLOWEST_RANK_BYTES'].tolist()
        })

        # Calculate total transfer size for each shared file access
        shared_files['TOTAL_TRANSFER_SIZE'] = shared_files['POSIX_BYTES_WRITTEN'] + shared_files['POSIX_BYTES_READ']

        # Calculate the load imbalance percentage for each shared file access
        shared_files['LOAD_IMBALANCE'] = abs(shared_files['POSIX_SLOWEST_RANK_BYTES'] - shared_files['POSIX_FASTEST_RANK_BYTES']) / shared_files['TOTAL_TRANSFER_SIZE']

        # Create a heatmap to visualize the load imbalance for each shared file access
        plt.figure(figsize=(10, 6))
        heatmap_data = shared_files.pivot('id', 'TOTAL_TRANSFER_SIZE', 'LOAD_IMBALANCE')
        sns.heatmap(heatmap_data, cmap='coolwarm', annot=True, fmt=".2f", cbar_kws={'label': 'Load Imbalance Percentage'})
        plt.title('Load Imbalance caused by Stragglers for Shared File Accesses')
        plt.xlabel('Total Transfer Size')
        plt.ylabel('Shared File ID')
        plt.tight_layout()
        plt.savefig('graph7.png')
        stragglers_count = 0

        shared_files = shared_files.assign(id=lambda d: d['id'].astype(str))

        # Get the files responsible
        detected_files = []

        for index, row in shared_files.iterrows():
            total_transfer_size = row['POSIX_BYTES_WRITTEN'] + row['POSIX_BYTES_READ']

            if total_transfer_size and abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size * 100
                ])

        if stragglers_count:
            issue = 'Detected data transfer imbalance caused by stragglers when accessing {} shared file.'.format(
                stragglers_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file_map[int(file[0])] if args.full_path else os.path.basename(file_map[int(file[0])])
                        ),
                        'graph' : 'graph7.png'
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks',
                    'graph' : 'graph7.png'
                },
                {
                    'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default'),
                    'graph' : 'graph7.png'
                }
            ]

            insights_operation.append(
                message(INSIGHTS_POSIX_SIZE_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
            )

        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME


        # Hypothetical data (replace with actual Darshan counter data)
        shared_files_times = pd.DataFrame({
            'id': df['counters']['jobid'].tolist(),
            'POSIX_F_WRITE_TIME': df['counters']['POSIX_F_WRITE_TIME'].tolist(),
            'POSIX_F_READ_TIME': df['counters']['POSIX_F_READ_TIME'].tolist(),
            'POSIX_F_META_TIME': df['counters']['POSIX_F_META_TIME'].tolist(),
            'POSIX_F_FASTEST_RANK_TIME': df['counters']['POSIX_F_FASTEST_RANK_TIME'].tolist(),
            'POSIX_F_SLOWEST_RANK_TIME': df['counters']['POSIX_F_SLOWEST_RANK_TIME'].tolist()
        })


        # Calculate total transfer time for each shared file access
        shared_files_times['TOTAL_TRANSFER_TIME'] = shared_files_times['POSIX_F_WRITE_TIME'] + shared_files_times['POSIX_F_READ_TIME'] + shared_files_times['POSIX_F_META_TIME']

        # Calculate the time imbalance percentage for each shared file access
        shared_files_times['TIME_IMBALANCE'] = abs(shared_files_times['POSIX_F_SLOWEST_RANK_TIME'] - shared_files_times['POSIX_F_FASTEST_RANK_TIME']) / shared_files_times['TOTAL_TRANSFER_TIME']

        # Create a heatmap to visualize the time imbalance for each shared file access
        plt.figure(figsize=(10, 6))
        heatmap_data = shared_files_times.pivot('id', 'TOTAL_TRANSFER_TIME', 'TIME_IMBALANCE')
        sns.heatmap(heatmap_data, cmap='coolwarm', annot=True, fmt=".2f", cbar_kws={'label': 'Time Imbalance Percentage'})
        plt.title('Time Imbalance caused by Stragglers for Shared File Accesses')
        plt.xlabel('Total Transfer Time')
        plt.ylabel('Shared File ID')
        plt.tight_layout()
        plt.savefig('graph8.png')
        shared_files_times = df['fcounters'].loc[(df['fcounters']['rank'] == -1)]

        # Get the files responsible
        detected_files = []

        stragglers_count = 0
        stragglers_imbalance = {}

        shared_files_times = shared_files_times.assign(id=lambda d: d['id'].astype(str))

        for index, row in shared_files_times.iterrows():
            total_transfer_time = row['POSIX_F_WRITE_TIME'] + row['POSIX_F_READ_TIME'] + row['POSIX_F_META_TIME']

            if total_transfer_time and abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time * 100
                ])

        if stragglers_count:
            issue = 'Detected time imbalance caused by stragglers when accessing {} shared file.'.format(
                stragglers_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file_map[int(file[0])] if args.full_path else os.path.basename(file_map[int(file[0])])
                        ),
                        'graph' : 'graph8.png'
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better distributing the data in the parallel file system',
                    'graph' : 'graph8.png' # needs to review what suggestion to give
                },
                {
                    'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default'),
                    'graph' : 'graph8.png'
                }
            ]

            insights_operation.append(
                message(INSIGHTS_POSIX_TIME_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
            )

        aggregated = df['counters'].loc[(df['counters']['rank'] != -1)][
            ['rank', 'id', 'POSIX_BYTES_WRITTEN', 'POSIX_BYTES_READ']
        ].groupby('id', as_index=False).agg({
            'rank': 'nunique',
            'POSIX_BYTES_WRITTEN': ['sum', 'min', 'max'],
            'POSIX_BYTES_READ': ['sum', 'min', 'max']
        })

        aggregated.columns = list(map('_'.join, aggregated.columns.values))

        aggregated = aggregated.assign(id=lambda d: d['id_'].astype(str))
        shared_files_times = pd.DataFrame({
            'id': df['counters']['jobid'].tolist(),
            'POSIX_F_WRITE_TIME': df['counters']['POSIX_F_WRITE_TIME'].tolist(),
            'POSIX_F_READ_TIME': df['counters']['POSIX_F_READ_TIME'].tolist(),
            'POSIX_F_META_TIME': df['counters']['POSIX_F_META_TIME'].tolist(),
            'POSIX_F_FASTEST_RANK_TIME': df['counters']['POSIX_F_FASTEST_RANK_TIME'].tolist(),
            'POSIX_F_SLOWEST_RANK_TIME': df['counters']['POSIX_F_SLOWEST_RANK_TIME'].tolist(),
        })
        # Calculate total transfer time for each shared file access
        shared_files_times['TOTAL_TRANSFER_TIME'] = shared_files_times['POSIX_F_WRITE_TIME'] + shared_files_times['POSIX_F_READ_TIME'] + shared_files_times['POSIX_F_META_TIME']

        # Calculate the time imbalance percentage for each shared file access
        shared_files_times['TIME_IMBALANCE'] = abs(shared_files_times['POSIX_F_SLOWEST_RANK_TIME'] - shared_files_times['POSIX_F_FASTEST_RANK_TIME']) / shared_files_times['TOTAL_TRANSFER_TIME']

        # Create a heatmap to visualize the time imbalance for each shared file access
        plt.figure(figsize=(10, 6))
        heatmap_data = shared_files_times.pivot('id', 'TOTAL_TRANSFER_TIME', 'TIME_IMBALANCE')
        sns.heatmap(heatmap_data, cmap='coolwarm', annot=True, fmt=".2f", cbar_kws={'label': 'Time Imbalance Percentage'})
        plt.title('Time Imbalance caused by Stragglers for Shared File Accesses')
        plt.xlabel('Total Transfer Time')
        plt.ylabel('Shared File ID')
        plt.tight_layout()
        plt.savefig('graph9.png')
        # Get the files responsible
        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_WRITTEN_max'] and abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row['POSIX_BYTES_WRITTEN_max'] > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row['POSIX_BYTES_WRITTEN_max'] * 100
                ])

        if imbalance_count:
            issue = 'Detected write imbalance when accessing {} individual files'.format(
                imbalance_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file_map[int(file[0])] if args.full_path else os.path.basename(file_map[int(file[0])])
                        ),
                        'graph' : 'graph9.png'
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks',
                    'graph' : 'graph9.png'
                },
                {
                    'message': 'Consider tuning the stripe size and count to better distribute the data',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default'),
                    'graph' : 'graph9.png'
                },
                {
                    'message': 'If the application uses netCDF and HDF5 double-check the need to set NO_FILL values',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/pnetcdf-hdf5-no-fill.c'), line_numbers=True, background_color='default'),
                    'graph' : 'graph9.png'
                },
                {
                    'message': 'If rank 0 is the only one opening the file, consider using MPI-IO collectives',
                    'graph' : 'graph9.png'
                }
            ]

            insights_operation.append(
                message(INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_READ_max'] and abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] * 100
                ])

        if imbalance_count:
            issue = 'Detected read imbalance when accessing {} individual files.'.format(
                imbalance_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file_map[int(file[0])] if args.full_path else os.path.basename(file_map[int(file[0])])
                        ),
                        'graph' : 'graph9.png'
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks',
                    'graph' : 'graph9.png'

                },
                {
                    'message': 'Consider tuning the stripe size and count to better distribute the data',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default'),
                    'graph' : 'graph9.png'
                },
                {
                    'message': 'If the application uses netCDF and HDF5 double-check the need to set NO_FILL values',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/pnetcdf-hdf5-no-fill.c'), line_numbers=True, background_color='default'),
                    'graph' : 'graph9.png'
                },
                {
                    'message': 'If rank 0 is the only one opening the file, consider using MPI-IO collectives',
                    'graph' : 'graph9.png'
                }
            ]

            insights_operation.append(
                message(INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

    #########################################################################################################################################################################

        # Hypothetical data (replace with actual data from the code snippet)
        # Calculate the percentage of collective read operations
        total_indep_reads = df_mpiio['counters']['MPIIO_INDEP_READS'].sum()
        total_coll_reads = df_mpiio['counters']['MPIIO_COLL_READS'].sum()
        total_reads = total_indep_reads + total_coll_reads

        # Calculate the percentage of collective read operations
        percentage_coll_reads = (total_coll_reads / total_reads) * 100

        # Plot the bar chart
        plt.figure(figsize=(8, 6))
        plt.bar(['Collective Reads', 'Independent Reads'], [percentage_coll_reads, 100 - percentage_coll_reads], color=['blue', 'orange'])
        plt.xlabel('Read Operations')
        plt.ylabel('Percentage')
        plt.title('Percentage of Collective Reads vs. Independent Reads')
        plt.ylim(0, 100)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig('graph10.png')
        
        # Assuming df_mpiio['counters']['MPIIO_COLL_READS'].sum() and df_mpiio['counters']['MPIIO_INDEP_READS'].sum() are already defined

        # Total MPI-IO read operations
        total_mpiio_read_operations = df_mpiio['counters']['MPIIO_COLL_READS'].sum() + df_mpiio['counters']['MPIIO_INDEP_READS'].sum()

        # Count of collective read operations
        collective_reads = df_mpiio['counters']['MPIIO_COLL_READS'].sum()

        # Count of independent read operations
        independent_reads = df_mpiio['counters']['MPIIO_INDEP_READS'].sum()

        # Create labels and sizes for the pie chart
        labels = ['Collective Reads', 'Independent Reads']
        sizes = [collective_reads, independent_reads]

        # Plotting the pie chart
        plt.figure(figsize=(6, 6))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['lightskyblue', 'lightcoral'])

        plt.title('MPI-IO Read Operations')
        plt.axis('equal')
        plt.savefig('graph13.png')
    if 'MPI-IO' in report.records:
        # Check if application uses MPI-IO and collective operations
        df_mpiio = report.records['MPI-IO'].to_df()

        df_mpiio['counters'] = df_mpiio['counters'].assign(id=lambda d: d['id'].astype(str))

        #print(df_mpiio)


        # Get the files responsible
        detected_files = []

        df_mpiio_collective_reads = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_READS'] > 0)]

        total_mpiio_read_operations = df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters']['MPIIO_COLL_READS'].sum()

        if df_mpiio['counters']['MPIIO_COLL_READS'].sum() == 0:
            if total_mpiio_read_operations and total_mpiio_read_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                issue = 'Application uses MPI-IO but it does not use collective read operations, instead it issues {} ({:.2f}%) independent read calls'.format(
                    df_mpiio['counters']['MPIIO_INDEP_READS'].sum(),
                    df_mpiio['counters']['MPIIO_INDEP_READS'].sum() / (total_mpiio_read_operations) * 100
                )

                detail = []

                files = pd.DataFrame(df_mpiio_collective_reads.groupby('id').sum()).reset_index()

                for index, row in df_mpiio_collective_reads.iterrows():
                    if (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > THRESHOLD_COLLECTIVE_OPERATIONS and (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                        detail.append(
                            {
                                'message': '{} ({}%) of independent reads to "{}"'.format(
                                    row['MPIIO_INDEP_READS'],
                                    row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100,
                                    file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                ),
                                'graph' : 'graph13.png'
                            }
                        )

                recommendation = [
                    {
                        'message': 'Use collective read operations (e.g. MPI_File_read_all() or MPI_File_read_at_all()) and set one aggregator per compute node',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph13.png'
                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )
        else:
            issue = 'Application uses MPI-IO and read data using {} ({:.2f}%) collective operations'.format(
                df_mpiio['counters']['MPIIO_COLL_READS'].sum(),
                df_mpiio['counters']['MPIIO_COLL_READS'].sum() / (df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters']['MPIIO_COLL_READS'].sum()) * 100
            )

            insights_operation.append(
                message(INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, OK, issue)
            )

        df_mpiio_collective_writes = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_WRITES'] > 0)]

        total_mpiio_write_operations = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()

        if df_mpiio['counters']['MPIIO_COLL_WRITES'].sum() == 0:
            if total_mpiio_write_operations and total_mpiio_write_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                issue = 'Application uses MPI-IO but it does not use collective write operations, instead it issues {} ({:.2f}%) independent write calls'.format(
                    df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum(),
                    df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() / (total_mpiio_write_operations) * 100
                )

                detail = []

                files = pd.DataFrame(df_mpiio_collective_writes.groupby('id').sum()).reset_index()

                for index, row in df_mpiio_collective_writes.iterrows():
                    if (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > THRESHOLD_COLLECTIVE_OPERATIONS and (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                        detail.append(
                            {
                                'message': '{} ({}%) independent writes to "{}"'.format(
                                    row['MPIIO_INDEP_WRITES'],
                                    row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100,
                                    file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                ),
                                'graph' : 'graph13.png'
                            }
                        )

                recommendation = [
                    {
                        'message': 'Use collective write operations (e.g. MPI_File_write_all() or MPI_File_write_at_all()) and set one aggregator per compute node',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph13.png'
                    }
                ]

                insights_operation.append(
                    message(INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )
        else:
            issue = 'Application uses MPI-IO and write data using {} ({:.2f}%) collective operations'.format(
                df_mpiio['counters']['MPIIO_COLL_WRITES'].sum(),
                df_mpiio['counters']['MPIIO_COLL_WRITES'].sum() / (df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()) * 100
            )

            insights_operation.append(
                message(INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, OK, issue)
            )

        #########################################################################################################################################################################
        # Total MPI-IO read operations (blocking + non-blocking)
        total_mpiio_reads = df_mpiio['counters']['MPIIO_NB_READS'].sum() + df_mpiio['counters']['MPIIO_IND_READS'].sum()

        # Total MPI-IO write operations (blocking + non-blocking)
        total_mpiio_writes = df_mpiio['counters']['MPIIO_NB_WRITES'].sum() + df_mpiio['counters']['MPIIO_IND_WRITES'].sum()

        # Percentage of non-blocking reads and blocking reads
        percentage_nb_reads = df_mpiio['counters']['MPIIO_NB_READS'].sum() / total_mpiio_reads * 100

        # Percentage of non-blocking writes and blocking writes
        percentage_nb_writes = df_mpiio['counters']['MPIIO_NB_WRITES'].sum() / total_mpiio_writes * 100

        # Plotting the graph
        labels = ['Blocking Reads', 'Non-blocking (Async) Reads', 'Blocking Writes', 'Non-blocking (Async) Writes']
        values = [total_mpiio_reads - df_mpiio['counters']['MPIIO_NB_READS'].sum(), df_mpiio['counters']['MPIIO_NB_READS'].sum(),
                total_mpiio_writes - df_mpiio['counters']['MPIIO_NB_WRITES'].sum(), df_mpiio['counters']['MPIIO_NB_WRITES'].sum()]
        colors = ['lightcoral', 'lightskyblue', 'lightcoral', 'lightskyblue']

        plt.figure(figsize=(10, 6))
        plt.pie(values, labels=labels, colors=colors, autopct='%.1f%%', startangle=140)
        plt.title('MPI-IO Read and Write Operations - Blocking vs. Non-blocking (Async)')
        plt.axis('equal')
        plt.savefig('graph11.png')
        # Look for usage of non-block operations

        # Look for HDF5 file extension

        has_hdf5_extension = False

        for index, row in df_mpiio['counters'].iterrows():
            if file_map[int(row['id'])].endswith('.h5') or file_map[int(row['id'])].endswith('.hdf5'):
                has_hdf5_extension = True

        if df_mpiio['counters']['MPIIO_NB_READS'].sum() == 0:
            issue = 'Application could benefit from non-blocking (asynchronous) reads'

            recommendation = []

            if 'H5F' in modules or has_hdf5_extension:
                recommendation.append(
                    {
                        'message': 'Since you use HDF5, consider using the ASYNC I/O VOL connector (https://github.com/hpc-io/vol-async)',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-vol-async-read.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph11.png'
                    }
                )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since you use MPI-IO, consider non-blocking/asynchronous I/O operations', # (e.g., MPI_File_iread(), MPI_File_read_all_begin/end(), or MPI_File_read_at_all_begin/end())',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-iread.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph11.png'
                    }
                )

            insights_operation.append(
                message(INSIGHTS_MPI_IO_BLOCKING_READ_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
            )

        if df_mpiio['counters']['MPIIO_NB_WRITES'].sum() == 0:
            issue = 'Application could benefit from non-blocking (asynchronous) writes'

            recommendation = []

            if 'H5F' in modules or has_hdf5_extension:
                recommendation.append(
                    {
                        'message': 'Since you use HDF5, consider using the ASYNC I/O VOL connector (https://github.com/hpc-io/vol-async)',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-vol-async-write.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph11.png'
                    }
                )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since you use MPI-IO, consider non-blocking/asynchronous I/O operations',  # (e.g., MPI_File_iwrite(), MPI_File_write_all_begin/end(), or MPI_File_write_at_all_begin/end())',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-iwrite.c'), line_numbers=True, background_color='default'),
                        'graph' : 'graph11.png'
                    }
                )

            insights_operation.append(
                message(INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
            )

    #########################################################################################################################################################################

    # Assuming cb_nodes and NUMBER_OF_COMPUTE_NODES are already defined correctly

    # Number of compute nodes
    x = ['Number of Aggregators', 'Number of Compute Nodes']
    values = [cb_nodes, NUMBER_OF_COMPUTE_NODES]

    # Plotting the graph
    plt.figure(figsize=(8, 6))
    plt.bar(x, values, color=['lightcoral', 'lightskyblue'])

    plt.xlabel('Status')
    plt.ylabel('Count')
    plt.title('MPI-IO Aggregators per Compute Node')
    plt.savefig('graph12.png')
    # Nodes and MPI-IO aggregators
    # If the application uses collective reads or collective writes, look for the number of aggregators
    hints = ''

    if 'h' in job['job']['metadata']:
        hints = job['job']['metadata']['h']

        if hints:
            hints = hints.split(';')

    # print('Hints: ', hints)

    #########################################################################################################################################################################

    NUMBER_OF_COMPUTE_NODES = 0

    if 'MPI-IO' in modules:
        cb_nodes = None

        for hint in hints:
            (key, value) = hint.split('=')
            
            if key == 'cb_nodes':
                cb_nodes = value

        # Try to get the number of compute nodes from SLURM, if not found, set as information
        command = 'sacct --job {} --format=JobID,JobIDRaw,NNodes,NCPUs --parsable2 --delimiter ","'.format(
            job['job']['jobid']
        )

        arguments = shlex.split(command)

        try:
            result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if result.returncode == 0:
                # We have successfully fetched the information from SLURM
                db = csv.DictReader(io.StringIO(result.stdout.decode('utf-8')))

                try:
                    first = next(db)

                    if 'NNodes' in first:
                        NUMBER_OF_COMPUTE_NODES = first['NNodes']

                        # Do we have one MPI-IO aggregator per node?
                        if cb_nodes > NUMBER_OF_COMPUTE_NODES:
                            issue = 'Application is using inter-node aggregators (which require network communication)'

                            recommendation = [
                                {
                                    'message': 'Set the MPI hints for the number of aggregators as one per compute node (e.g., cb_nodes={})'.format(
                                        NUMBER_OF_COMPUTE_NODES
                                    ),
                                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-hints.bash'), line_numbers=True, background_color='default'),
                                    'graph' : 'graph12.png'
                                }
                            ]

                            insights_operation.append(
                                message(INSIGHTS_MPI_IO_AGGREGATORS_INTER, TARGET_USER, HIGH, issue, recommendation)
                            )

                        if cb_nodes < NUMBER_OF_COMPUTE_NODES:
                            issue = 'Application is using intra-node aggregators'

                            insights_operation.append(
                                message(INSIGHTS_MPI_IO_AGGREGATORS_INTRA, TARGET_USER, OK, issue)
                            )

                        if cb_nodes == NUMBER_OF_COMPUTE_NODES:
                            issue = 'Application is using one aggregator per compute node'

                            insights_operation.append(
                                message(INSIGHTS_MPI_IO_AGGREGATORS_OK, TARGET_USER, OK, issue)
                            )


                except StopIteration:
                    pass
        except FileNotFoundError:
            pass
    
    #########################################################################################################################################################################
    
    codes = []
    if args.json:
        f = open(args.json)
        data = json.load(f)

        for key, values in data.items():
            for value in values:
                code = value['code']
                codes.append(code)

                level = value['level']
                issue = value['issue']
                recommendation = []
                for rec in value['recommendations']:
                    new_message = {'message': rec}
                    recommendation.append(new_message)

                insights_dxt.append(
                    message(code, TARGET_DEVELOPER, level, issue, recommendation)
                )

    #########################################################################################################################################################################

    insights_end_time = time.time()

    # Version 3.4.1 of py-darshan changed the contents on what is reported in 'job'
    if 'start_time' in job['job']:
        job_start = datetime.datetime.fromtimestamp(job['job']['start_time'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(job['job']['end_time'], datetime.timezone.utc)
    else:
        job_start = datetime.datetime.fromtimestamp(job['job']['start_time_sec'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(job['job']['end_time_sec'], datetime.timezone.utc)

    console.print()

    console.print(
        Panel(
            '\n'.join([
                ' [b]JOB[/b]:            [white]{}[/white]'.format(
                    job['job']['jobid']
                ),
                ' [b]EXECUTABLE[/b]:     [white]{}[/white]'.format(
                    job['exe'].split()[0]
                ),
                ' [b]DARSHAN[/b]:        [white]{}[/white]'.format(
                    os.path.basename(args.darshan)
                ),
                ' [b]EXECUTION TIME[/b]: [white]{} to {} ({:.2f} hours)[/white]'.format(
                    job_start,
                    job_end,
                    (job_end - job_start).total_seconds() / 3600
                ),
                ' [b]FILES[/b]:          [white]{} files ({} use STDIO, {} use POSIX, {} use MPI-IO)[/white]'.format(
                    total_files,
                    total_files_stdio,
                    total_files_posix - total_files_mpiio,  # Since MPI-IO files will always use POSIX, we can decrement to get a unique count
                    total_files_mpiio
                ),
                ' [b]COMPUTE NODES[/b]   [white]{}[/white]'.format(
                    NUMBER_OF_COMPUTE_NODES
                ),
                ' [b]PROCESSES[/b]       [white]{}[/white]'.format(
                    job['job']['nprocs']
                ),
                ' [b]HINTS[/b]:          [white]{}[/white]'.format(
                    ' '.join(hints)
                )
            ]),
            title='[b][slate_blue3]DRISHTI[/slate_blue3] v.0.3[/b]',
            title_align='left',
            subtitle='[red][b]{} critical issues[/b][/red], [orange1][b]{} warnings[/b][/orange1], and [white][b]{} recommendations[/b][/white]'.format(
                insights_total[HIGH],
                insights_total[WARN],
                insights_total[RECOMMENDATIONS],
            ),
            subtitle_align='left',
            padding=1
        )
    )

    console.print()

    if insights_metadata:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_metadata
                    ),
                    (1, 1)
                ),
                title='METADATA',
                title_align='left'
            )
        )

    if insights_operation:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_operation
                    ),
                    (1, 1)
                ),
                title='OPERATIONS',
                title_align='left'
            )
        )

    if insights_dxt:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_dxt
                    ),
                    (1, 1)
                ),
                title='DXT',
                title_align='left'
            )
        )
        
    console.print(
        Panel(
            ' {} | [white]LBNL[/white] | [white]Drishti report generated at {} in[/white] {:.3f} seconds'.format(
                datetime.datetime.now().year,
                datetime.datetime.now(),
                insights_end_time - insights_start_time
            ),
            box=box.SIMPLE
        )
    )

    if args.export_theme_light:
        export_theme = TerminalTheme(
            (255, 255, 255),
            (0, 0, 0),
            [
                (26, 26, 26),
                (244, 0, 95),
                (152, 224, 36),
                (253, 151, 31),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (120, 120, 120),
                (98, 94, 76),
            ],
            [
                (244, 0, 95),
                (152, 224, 36),
                (224, 213, 97),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (246, 246, 239),
            ],
        )
    else:
        export_theme = MONOKAI

    if args.export_html:
        console.save_html(
            '{}.html'.format(args.darshan),
            theme=export_theme,
            clear=False
        )

    if args.export_svg:
        console.save_svg(
            '{}.svg'.format(args.darshan),
            title='Drishti',
            theme=export_theme,
            clear=False
        )

    if args.export_csv:
        issues = [
            'JOB',
            INSIGHTS_STDIO_HIGH_USAGE,
            INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE,
            INSIGHTS_POSIX_READ_COUNT_INTENSIVE,
            INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE,
            INSIGHTS_POSIX_READ_SIZE_INTENSIVE,
            INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE,
            INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE,
            INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE,
            INSIGHTS_POSIX_REDUNDANT_READ_USAGE,
            INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE,
            INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE,
            INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE,
            INSIGHTS_POSIX_HIGH_METADATA_TIME,
            INSIGHTS_POSIX_SIZE_IMBALANCE,
            INSIGHTS_POSIX_TIME_IMBALANCE,
            INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE,
            INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE,
            INSIGHTS_MPI_IO_NO_USAGE,
            INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE,
            INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE,
            INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE,
            INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE,
            INSIGHTS_MPI_IO_BLOCKING_READ_USAGE,
            INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE,
            INSIGHTS_MPI_IO_AGGREGATORS_INTRA,
            INSIGHTS_MPI_IO_AGGREGATORS_INTER,
            INSIGHTS_MPI_IO_AGGREGATORS_OK
        ]
        if codes:
            issues.extend(codes)

        detected_issues = dict.fromkeys(issues, False)
        detected_issues['JOB'] = job['job']['jobid']

        for report in csv_report:
            detected_issues[report] = True

        filename = '{}-summary.csv'.format(
            args.darshan.replace('.darshan', '')
        )

        with open(filename, 'w') as f:
            w = csv.writer(f)
            w.writerow(detected_issues.keys())
            w.writerow(detected_issues.values())


if __name__ == '__main__':
    main()