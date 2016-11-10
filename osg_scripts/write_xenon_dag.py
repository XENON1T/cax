#!/usr/bin/env python
from __future__ import print_function
import os
import logging as log
from optparse import OptionParser


"""
This script creates an HTCondor DAG file by walking through a given
input directory and looking for folders that contain files that follow
an input pattern.

This script assumes a directory structure of

/path/to/file/foo/bar/YYMMDD_HHMM/XENON1T-*.zip

Top level dir = /path/to/file/foo/bar/
run directory = YYMMDD_HHMM

uri = gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm
is the Uniform Resource Identifier that shows

The scripts has multiple input options:

`-o/--outdagfile`: (Required, and unique) DAG file to fill
`-v/--verbosity`: Verbosity of logging on a range from 1 to 4
`--inputdir`: (Required) Top level, which contains the run directory
`--outputdir`: (Required) Directory where output files will be put adding
               the YYMMDD_HHMM identifier
`--uri`: (Required) Uniform Resource Identifier that provides the gridFTP
         or srm server location and the mount point for the data
         on the server
`--inputfilefilter`: (Required) File identifier, for example XENON1T-
`--runnumbers`: A list of run numbers, i.e. YYMMDD_HHMM, to process
`--muonveto`: Process muon veto file
`--submitfile`: (Required) HTCondor submit file to be used
`--paxversion`: (Required) pax version to be used
"""


def get_out_name(filename):
    """
    Getting output file name by splitting a input file.
    """
    return os.path.splitext(filename)[0] + ".root"


def get_run_number(dir_name):
    """
    Getting run number from directory
    """
    return dir_name.split("/")[-1]


def callback_optparse(option, opt_str, value, parser):
    """
    Allow OptionParser in Python2.6 to have variable length
    lists of arguments to an option. Equivalent in Python2.7
    is nargs="+"
    """
    args = []
    for arg in parser.rargs:
        if arg[0] != "-":
            args.append(arg)
        else:
            del parser.rargs[:len(args)]
            break
    if getattr(parser.values, option.dest):
        args.extend(getattr(parser.values, option.dest))
    setattr(parser.values, option.dest, args)


def write_dag_file(options):
    """
    Writing DAG file.
    """
    i = 0
    with open(options.outdagfile, "wt") as dag_file:
        for dir_name, subdir_list, file_list in os.walk(options.inputdir):
            if not options.run_muonveto and "MV" in dir_name:
                continue
            run_number = get_run_number(dir_name)
            if (options.runnumbers is not None and
               run_number not in options.runnumbers):
                continue
            for infile in file_list:
                if options.inputfilefilter not in infile:
                    continue
                run_number = get_run_number(dir_name)
                outfile = get_out_name(infile)
                infile = os.path.join(options.uri, dir_name, infile)
                if not os.path.exists(os.path.join(options.outdir,
                                                   run_number)):
                    os.makedirs(os.path.join(options.outdir, run_number))
                outfile = os.path.join(options.uri, options.outdir,
                                       run_number, outfile)
                dag_file.write("JOB XENON.%d %s\n" % (i, options.submitfile))
                dag_file.write(("VARS XENON.%d input_file=\"%s\" "
                                "out_location=\"%s\" name=\"%s\" "
                                "ncpus=\"1\" disable_updates=\"True\" "
                                "host=\"login\" pax_version=\"%s\" "
                                "pax_hash=\"n/a\"\n") % (i, infile,
                                                         outfile, run_number,
                                                         options.paxversion))
                dag_file.write("Retry XENON.%d 3\n" % i)
                i += 1


def main(options, args):
    if options.outdagfile is None:
        log.fatal("No output DAG file provided")
        raise RuntimeError()
    else:
        if os.path.exists(options.outdagfile):
            log.fatal("Output DAG file exists. Please rename or delete.")
            raise RuntimeError()
    if options.inputdir is None:
        log.fatal("No input dir provided")
        raise RuntimeError()
    if options.outdir is None:
        log.fatal("No output top level dir provided")
        raise RuntimeError()
    if options.uri is None:
        log.fatal("No URI to file transfer server provided")
        raise RuntimeError()
    if options.submitfile is None:
        log.fatal("No submit file provided")
        raise RuntimeError()
    else:
        if not os.path.exists(options.submitfile):
            log.fatal("Submit file does not exist. Please create.")
            raise RuntimeError()
    if options.paxversion is None:
        log.fatal("No URI to file transfer server provided")
        raise RuntimeError()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-o", "--outdagfile", dest="outdagfile", default=None,
                      help="filename of dag file to write to",)
    parser.add_option("-v", "--verbosity", dest="verbosity",
                      help="Set log level", default=4)
    parser.add_option("--inputdir", dest="inputdir", default=None,
                      help=("Top level input dir to walk "
                            "through and find input files"))
    parser.add_option("--outputdir", dest="outputdir", default=None,
                      help="Force update information")
    parser.add_option("--uri", dest="uri", default=None,
                      help="Force update information")
    parser.add_option("--inputfilefilter", dest="inputfilefilter",
                      default="XENON1T-", help=("Filter by which to "
                                                "limit # of input files"))
    parser.add_option("--runnumbers", dest="runnumbers", default=None,
                      action="callback", callback=callback_optparse,
                      help="Run numbers to consider")
    parser.add_option("--muonveto", dest="run_muonveto", action="store_true",
                      default=False, help="Process Muon Veto data ")
    parser.add_option("--submitfile", dest="submitfile", default=None,
                      help="Submit file to be used")
    parser.add_option("--paxversion", dest="paxversion", default=None,
                      help="PAX version to be used")
    (options, args) = parser.parse_args()
    level = {
        1: logging.ERROR,
        2: logging.WARNING,
        3: logging.INFO,
        4: logging.DEBUG
    }.get(options.verbosity, logging.DEBUG)
    logging.basicConfig(level=level)
    main(options, args)