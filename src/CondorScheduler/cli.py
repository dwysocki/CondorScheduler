from argparse import ArgumentParser
from os import path
import sys
from subprocess import check_call

from . import condor, dag, input_grouper, util



def get_args():
    # create CLI argument parser
    parser = ArgumentParser()

    # divide arguments into groups
    general = parser.add_argument_group("General")
    batch   = parser.add_argument_group("Batch Script")
    pre     = parser.add_argument_group("Pre Script")
    post    = parser.add_argument_group("Post Script")

    #############
    ## General ##
    #############
    general.add_argument("--min-size", type=int,
        default=10,
        help="Minimum number of lines to process in a single job. "
             "(default 10)")
    general.add_argument("--max-jobs", type=int,
        help="Maximum number of Condor jobs to schedule. "
             "(optional)")
    general.add_argument("-d", "--condor-dir",
        default="./condor_scheduler",
        help="Directory to store all Condor files without specified paths. "
             "(default ./condor_scheduler)")
    general.add_argument("--submit", action="store_true",
        help="Submit Condor jobs after creation. "
             "(optional)")
    general.add_argument("--dag",
        help="File to write generated DAG to. "
             "(default <condor-dir>/generated.dag)")
    general.add_argument("--dag-args",
        default="",
        help="Arguments to pass to condor_submit_dag before DAG filename. "
             "(optional)")
    general.add_argument("--dot",
        help="Graphviz output file for DAG visualization. "
             "(optional)")

    ##################
    ## Batch Script ##
    ##################
    batch.add_argument("--batch-script",
        required=True,
        help="Batch script to execute.")
    batch.add_argument("--batch-args",
        default="",
        help="Arguments to batch script. "
             "(optional)")
    batch.add_argument("--batch-input-dir",
        help="Directory to populate with separated input files. "
             "(default <condor-dir>/input/)")
    batch.add_argument("--batch-output",
        help="Output file for batch script. "
             "(default <condor-dir>/batch.out.$(cluster).$(process))")
    batch.add_argument("--batch-error",
        help="Error file for batch script. "
             "(default <condor-dir>/batch.err.$(cluster).$(process))")
    batch.add_argument("--batch-log",
        help="Log file for batch script. "
             "(default <condor-dir>/batch.log.$(cluster).$(process))")
    batch.add_argument("--batch-condor",
        help="Condor file to generate for batch script. "
             "(default <condor-dir>/batch.condor)")
    batch.add_argument("--batch-universe",
        default="vanilla",
        help="Universe for batch script. "
             "(default vanilla)")

    ################
    ## Pre Script ##
    ################
    pre.add_argument("--pre-script",
        help="Script to execute before batch script. "
             "(optional)")
    pre.add_argument("--pre-args",
        default="",
        help="Arguments to pre script. "
             "(optional)")
    pre.add_argument("--pre-input",
        help="Input file for pre script. "
             "(required if --pre-script provided).")
    pre.add_argument("--pre-output",
        help="Output file for pre script. "
             "(default <condor-dir>/pre.out)")
    pre.add_argument("--pre-error",
        help="Error file for pre script. "
             "(default <condor-dir>/pre.err)")
    pre.add_argument("--pre-log",
        help="Log file for pre script. "
             "(default <condor-dir>/pre.log)")
    pre.add_argument("--pre-condor",
        help="Condor file to generate for pre script. "
             "(default <condor-dir>/pre.condor)")
    pre.add_argument("--pre-universe",
        default="vanilla",
        help="Universe for pre script. "
             "(default vanilla)")

    #################
    ## Post Script ##
    #################
    post.add_argument("--post-script",
        help="Script to execute after batch script. "
             "(optional)")
    post.add_argument("--post-args",
        default="",
        help="Arguments to post script. "
             "(optional)")
    post.add_argument("--post-input",
        help="Input file for post script. "
             "(required if --post-script provided)")
    post.add_argument("--post-output",
        help="Output file for post script. "
             "(default <condor-dir>/post.out)")
    post.add_argument("--post-error",
        help="Error file for post script. "
             "(default <condor-dir>/post.err)")
    post.add_argument("--post-log",
        help="Log file for post script. "
             "(default <condor-dir>/post.log)")
    post.add_argument("--post-condor",
        help="Condor file to generate for post script. "
             "(default <condor-dir>/post.condor)")
    post.add_argument("--post-universe",
        default="vanilla",
        help="Universe for post script. "
             "(default vanilla).")

    args = parser.parse_args()

    # fill in defaults for optional filenames
    for prefix in ["batch", "pre", "post"]:
        fill_defaults(args, prefix)

    # confirm that input files provided for --pre- and --post-,
    # if scripts have been provided for them
    for prefix in ["pre", "post"]:
        confirm_inputs(args, prefix)

    for dir in [args.condor_dir, args.batch_input_dir]:
        util.make_sure_path_exists(dir)

    return args


def fill_defaults(args, prefix):
    """
    Fill default values for either the --batch-*, --pre-*, or --post-* options.

    Parameters
    ----------

    args : argparse.Namespace
        Parsed command line arguments.
    prefix : str
        Either "batch", "pre", or "post".

    Returns
    -------

    None
    """
    def set_default(attr, file_basename):
        if getattr(args, attr) is None:
            file_name = path.join(args.condor_dir, file_basename)
            setattr(args, attr, file_name)

    suffix = ".$(cluster).$(process)" if prefix == "batch" else ""

    script = prefix+"_script"
    output = prefix+"_output"
    error  = prefix+"_error"
    log    = prefix+"_log"
    condor = prefix+"_condor"



    if getattr(args, script) is not None:
        set_default(output, prefix+".out"+suffix)
        set_default(error,  prefix+".err"+suffix)
        set_default(log,    prefix+".log"+suffix)
        set_default(condor, prefix+".condor")

    if prefix == "batch":
        input_dir = prefix+"_input_dir"
        set_default(input_dir, "input")

    # DIRTY HACK WARNING:
    #   set default DAG file here, even though it's unrelated to prefix
    set_default("dag", "generated.dag")

def confirm_inputs(args, prefix):
    """
    If --<prefix>-script is provided, raise an exception if --<prefix>-input
    is not.

    Parameters
    ----------

    args : argparse.Namespace
        Parsed command line arguments.
    prefix : str
        Either "pre" or "post".

    Returns
    -------

    None
    """
    script = prefix+"_script"
    input  = prefix+"_input"
    if ((getattr(args, script) is not None) and
        (getattr(args, input) is None)):
        raise Exception(
            "input file required for {}-script {}"
            .format(prefix, getattr(args, script))
        )


def condor_template(args, prefix):
    universe   = getattr(args, prefix+"_universe")
    executable = getattr(args, prefix+"_script")
    arguments  = "$(arguments)"
    input      = "$(input)"
    output     = getattr(args, prefix+"_output")
    error      = getattr(args, prefix+"_error")
    log        = getattr(args, prefix+"_log")

    return condor.template(universe=universe,
                           executable=executable, arguments=arguments,
                           input=input, output=output, error=error, log=log)


def make_condor_files(args):
    return {
        prefix: condor_template(args, prefix)
        for prefix in ["batch", "pre", "post"]
        if getattr(args, prefix+"_script") is not None
    }


def make_dag_file(args, input_filenames):
    # initialize contents as empty string to be appended to
    contents = ""
    # initialize map of job ID's
    #
    # pre and post values may remain as None,
    # or become lists ["pre"] and ["post"]
    #
    # batch is known ahead of time to be [0, 1, ..., len(input_filenames)]
    # given as strings, for consistency
    job_ids = {
        "pre"   : None,
        "batch" : [str(x) for x in range(len(input_filenames))],
        "post"  : None
    }

    # append JOB and VARS entries for the pre and post scripts
    for prefix in ["pre", "post"]:
        if getattr(args, prefix+"_script") is not None:
            condor    = getattr(args, prefix+"_condor")
            input     = getattr(args, prefix+"_input")
            arguments = getattr(args, prefix+"_args")
            # give the job the same ID as the prefix
            job_ids[prefix] = [prefix]
            contents += dag.make_job(prefix, condor, input, arguments)

    # append JOB and VARS entries for each batch input
    for i, fname in enumerate(input_filenames):
        contents += dag.make_job(i, args.batch_condor, fname, args.batch_args)

    # append PARENT-CHILD entries
    if job_ids["pre"] is not None:
        contents += dag.parent_template(parents=job_ids["pre"],
                                        children=job_ids["batch"])
    if job_ids["post"] is not None:
        contents += dag.parent_template(parents=job_ids["batch"],
                                        children=job_ids["post"])

    # append DOT entry to output visualization
    if args.dot:
        contents += dag.dot_template(dot=args.dot)

    return contents


def save_dag_file(contents, fname):
    with open(fname, "w") as f:
        f.write(contents)


def save_condor_file(contents, args, prefix):
    fname = getattr(args, prefix+"_condor")

    with open(fname, "w") as f:
        f.write(contents)


def submit_dag(fname, options):
    system_call = ["condor_submit_dag"] + options.split() + [fname]
    check_call(system_call)


def main():
    """
    Main program logic.
    """
    args = get_args()

    # split lines from stdin into input files
    # saves each file to disc, and produces a list of their file paths
    input_file_names = input_grouper.make_files(sys.stdin.readlines(),
                                                args.batch_input_dir,
                                                args.min_size, args.max_jobs)

    # create mapping from prefix -> Condor file contents
    condor_file_contents_map = make_condor_files(args)

    # create Condor DAG file contents,
    dag_file_contents = make_dag_file(args, input_file_names)

    # save Condor files
    for prefix, contents in condor_file_contents_map.items():
        save_condor_file(contents, args, prefix)

    # save DAG file
    save_dag_file(dag_file_contents, args.dag)

    if args.submit:
        submit_dag(args.dag, args.dag_args)

#    print_contents(condor_file_contents_map)
#    print_contents({"DAG": dag_file_contents})




def print_contents(dict):
    for prefix, contents in dict.items():
        print("#### "+prefix+" ####")
        print(contents)
