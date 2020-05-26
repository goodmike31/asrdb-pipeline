from prefect import task, Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.engine.result_handlers import LocalResultHandler
from prefect.tasks.shell import ShellTask
from prefect.utilities.debug import raise_on_exception
from os.path import join
import os
import sys
import requests
import datetime
from pathlib import Path
import zipfile

test = ShellTask(name="print_date", command="date")

# TODO: read config with paths
# TODO: design tests for config file

dir_datalake = "/home/pnowak/development/data/asr/datalake"

# db_name_run_t = "clarin"
# db_lang_run = "pl-PL"
# url_run = "https://clarin-pl.eu/dspace/bitstream/handle/11321/237/Mobile.zip?sequence=2&isAllowed=y"
# db_name_run = "librispeech"
# db_lang_run = "en-US"
# url_run = "http://www.openslr.org/resources/12/dev-clean.tar.gz"
#f = open("config.tsv", "r")
#for line in f:
#    print(line)
#print(f.readline())
#f.close()

db_name_run = "crowdsourced_hq_arg_speech"
lang_run = "es-AR"
url_run = "http://www.openslr.org/resources/61/es_ar_male.zip"


# COMMON FUNCTIONS
dt = datetime.datetime.now()
date = dt.strftime("%Y%m%d")

def list_files(startpath):
    print("Making directory tree")
    print(startpath)
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        # for f in files:
        # print('{}{}'.format(subindent, f))

def download_file(url: str, path_dl: str):
    """Download file for given URL into given directory."""

    if (not os.path.exists(path_dl)):
        print('Saving as:\n%s' % (path_dl))
        with open(path_dl, 'wb') as f:
            response = requests.get(url, stream=True)
            total = response.headers.get('content-length')

            if total is None:
                f.write(response.content)
            else:
                downloaded = 0
                total = int(total)
                for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                    downloaded += len(data)
                    f.write(data)
                    done = int(50*downloaded/total)
                    sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50-done)))
                    sys.stdout.flush()
        sys.stdout.write('\n')
        print("Download completed")
    else:
        print ("File already downloaded.")


def target_dir_creator(task_name):
    """Create target dir for prefect task.

    Generate target dir for given task name and runtime parameters.
    """
    #print (type(task_name))
    target_dir = join(dir_datalake, db_name_run, lang_run, task_name)

    if not os.path.exists(target_dir):
        print("Creating dir:\n%s" % (target_dir))
        os.makedirs(target_dir)

    return target_dir

def extract_to_target_dir(path_to_zip_file, directory_to_extract_to):
    # check extension of archive
    filepath, extension = os.path.splitext(path_to_zip_file)
    filename = os.path.basename(filepath)
    path_to_extracted_data = os.path.join(directory_to_extract_to, filename)
    status_file = path_to_extracted_data + ".done"
    if (not os.path.exists(status_file)):
        if (extension == ".zip"):
            # extract zip
            with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
                zip_ref.extractall(path_to_extracted_data)
            print("Extraction completed")
            p = Path(status_file)
            p.touch()
        else:
            print("No handler for files with extension: ", extension)
    else:
        print ("File already extracted.")
    print(path_to_extracted_data)
    return path_to_extracted_data

# PREFECT TASKS
# @task(log_stdout=True, checkpoint=True,
# result_handler = LocalResultHandler(dir = "/home/pnowak/development/data/workspace/asrdb-pipeline"))

@task
def download(url: str, db_name: str, lang: str)-> str:
    """Download speech corpora archive.

    Download archive from given URL.
    Return path to downloaded archive.
    """
    # get name of the current prefect task
    task_name = "download"
    # check if URL download path is not empty
    assert(len(url) > 0)
    print('Requesting download:\n%s' % url)

    # create directory for download and target path for download function
    # consists of data lake directory, name of DB and lang

    # get filename, assuming it's located at the end of URL
    filename = os.path.basename(url)
    # truncate any extra URL parameters
    filename = filename.split("?")[0]
    # check if file name is valid
    # TODO check all forbidden charaters, length etc. - maybe some library?
    target_dir = target_dir_creator(task_name)
    # create target file location from datalake dir and extracted filename
    path_dl = os.path.join(target_dir, filename)
    # TODO replace manually checked status file passed as argument with
    # native target with varying filename

    download_file(url, path_dl)

    # return 3
    return path_dl


@task
def extract(path_to_archive):
    """Extract speech corpora archive.
    Extract archive for a given path.
    Return path to extracted archive.
    """
    task_name = "extract"
    target_dir = target_dir_creator(task_name)
    print("Extracting:\n%s\nto:\n%s" % (path_to_archive, target_dir))
    path_to_dir = extract_to_target_dir(path_to_archive, target_dir)
    return path_to_dir

@task(log_stdout=True)
def inspect (path_to_folder: str) -> str:
    """Inspect original content and structre of the speech corpora.
    """
    # get name of the current prefect task
    task_name = "inspect"
    # check if path to audio archive is valid
    target_dir = target_dir_creator(task_name)

    print("Inspecting:\n%s\n" % (path_to_folder))
    list_files(path_to_folder)

    return path_to_folder

with Flow('ASRDB Pipeline') as flow:
    url = Parameter('url')
    db_name = Parameter('db_name')
    lang = Parameter('lang')

    #TODO fix passing results of tasks
    path_data_raw = download(url=url, db_name=db_name, lang=lang)
    path_data_extracted = extract(path_data_raw)
    path_data_inspected = inspect(path_data_extracted)

#with raise_on_exception():
    # flow.visualize()
#    state = flow.run(parameters=dict(url=url_run, db_name=db_name_run, lang=lang_run))

flow.register()
